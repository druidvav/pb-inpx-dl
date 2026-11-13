use std::cell::RefCell;
use std::ffi::{CStr, c_char};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};
use std::thread;

use anyhow::{Context, Result, anyhow, ensure};
use inkview::bindings::{
    FONT_TYPE, FONT_TYPE_FONT_BOLD, FONT_TYPE_FONT_FAMILY, FONT_TYPE_FONT_STD, Inkview, KBD_NORMAL,
    KBD_PASSWORD, NET_CONNECTED, NET_OK, REQ_OPENBOOK,
};
use inkview::screen::WhichScreen;
use inkview_slint::Event;
use slint::fontique_010::{
    fontique::{Collection, FamilyId, GenericFamily},
    shared_collection,
};

const KEYBOARD_LIMIT: usize = 240;
const COMMAND_INTERVAL_MS: i32 = 150;
const FIRMWARE_FONT_DIRS: [&str; 3] = [
    "/ebrmain/share/fonts",
    "/ebrmain/fonts",
    "/ebrmain/fonts/pdf",
];

type Command = Box<dyn FnOnce(&Inkview) + Send>;
type KeyboardCallback = Box<dyn FnOnce(String)>;

static COMMANDS: OnceLock<Sender<Command>> = OnceLock::new();

thread_local! {
    static NATIVE: RefCell<Option<NativeLoop>> = const { RefCell::new(None) };
    static KEYBOARD_CALLBACK: RefCell<Option<KeyboardCallback>> = const { RefCell::new(None) };
}

struct NativeLoop {
    iv: &'static Inkview,
    commands: Receiver<Command>,
    events: Sender<Event>,
    ui_finished: Arc<AtomicBool>,
    keyboard: Box<[u8; KEYBOARD_LIMIT * 4 + 1]>,
    keyboard_active: bool,
}

/// Runs InkView on the calling thread and initializes Slint after EVT_INIT.
/// `init` creates the window and runs its Slint event loop.
pub fn run(init: impl FnOnce() -> Result<()> + Send + 'static) -> Result<()> {
    let iv: &'static Inkview = Box::leak(Box::new(inkview::load()));
    ensure!(iv.SetHardTimer.is_ok(), "InkView timer is unavailable");
    ensure!(iv.CloseApp.is_ok(), "InkView app shutdown is unavailable");
    let (events, incoming_events) = mpsc::channel();
    let (commands, incoming_commands) = mpsc::channel();
    let (font_names, incoming_font_names) = mpsc::channel();
    COMMANDS
        .set(commands)
        .map_err(|_| anyhow!("PocketBook event loop is already initialized"))?;

    let ui_finished = Arc::new(AtomicBool::new(false));
    let ui_thread = thread::Builder::new()
        .name("slint-ui".into())
        .spawn({
            let events = events.clone();
            let ui_finished = Arc::clone(&ui_finished);
            move || {
                let _finished = UiFinished(ui_finished);
                ensure!(
                    matches!(
                        incoming_events.recv(),
                        Ok(Event::InkviewEvent(inkview::Event::Init))
                    ),
                    "InkView closed before initializing the screen"
                );
                let backend: Box<dyn slint::platform::Platform> = match WhichScreen::new(iv) {
                    WhichScreen::BB8(screen) => {
                        Box::new(inkview_slint::Backend::new(screen, events, incoming_events))
                    }
                    WhichScreen::RGB24(screen) => {
                        Box::new(inkview_slint::Backend::new(screen, events, incoming_events))
                    }
                };
                slint::platform::set_platform(backend)?;
                register_system_fonts(incoming_font_names.recv().unwrap_or_default());
                init()
            }
        })
        .context("start Slint thread")?;

    NATIVE.with(|native| {
        native.replace(Some(NativeLoop {
            iv,
            commands: incoming_commands,
            events: events.clone(),
            ui_finished,
            keyboard: Box::new([0; KEYBOARD_LIMIT * 4 + 1]),
            keyboard_active: false,
        }));
    });

    let native_events = events.clone();
    inkview::iv_main(iv, move |event| {
        if event == inkview::Event::Init {
            let _ = font_names.send(
                [
                    FONT_TYPE_FONT_FAMILY,
                    FONT_TYPE_FONT_STD,
                    FONT_TYPE_FONT_BOLD,
                ]
                .map(|kind| firmware_font_name(iv, kind)),
            );
            schedule_commands(iv);
        }
        let _ = native_events.send(Event::InkviewEvent(event));
        Some(())
    });

    drop(NATIVE.with(|native| native.take()));
    let _ = events.send(Event::Quit);
    ui_thread
        .join()
        .map_err(|_| anyhow!("Slint thread panicked"))?
}

fn firmware_font_name(iv: &Inkview, kind: FONT_TYPE) -> Option<String> {
    let get_font = iv.iv_get_default_font.as_ref().ok()?;
    let name = unsafe { get_font(kind) };
    if name.is_null() {
        return None;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_str().ok()?.trim();
    (!name.is_empty()).then(|| name.to_owned())
}

fn register_system_fonts([family_name, regular, bold]: [Option<String>; 3]) {
    let mut fonts = shared_collection();
    let regular = regular
        .as_deref()
        .and_then(|name| register_firmware_face(&mut fonts, name));
    if let Some(name) = bold.as_deref() {
        register_firmware_face(&mut fonts, name);
    }
    let family = family_name
        .as_deref()
        .and_then(|name| fonts.family_by_name(name))
        .map(|family| family.id())
        .or(regular)
        .or_else(|| register_fallback_fonts(&mut fonts));
    if let Some(family) = family {
        for generic in [GenericFamily::SansSerif, GenericFamily::SystemUi] {
            fonts.set_generic_families(generic, std::iter::once(family));
        }
    }
}

fn register_firmware_face(fonts: &mut Collection, name: &str) -> Option<FamilyId> {
    let path = Path::new(name);
    if path.is_absolute() {
        let bytes = fs::read(path).ok()?;
        return fonts
            .register_fonts(bytes.into(), None)
            .first()
            .map(|face| face.0);
    }
    let name = path.file_stem()?.to_str()?;
    let compact_name = name.replace(' ', "");
    for directory in FIRMWARE_FONT_DIRS {
        for stem in [name, &compact_name] {
            for suffix in ["", "-Regular"] {
                for extension in ["ttf", "otf", "ttc"] {
                    let path = Path::new(directory).join(format!("{stem}{suffix}.{extension}"));
                    if let Ok(bytes) = fs::read(path)
                        && let Some((family, _)) = fonts.register_fonts(bytes.into(), None).first()
                    {
                        return Some(*family);
                    }
                }
            }
        }
    }
    None
}

fn register_fallback_fonts(fonts: &mut Collection) -> Option<FamilyId> {
    // Fontconfig may omit PocketBook's own font directories.
    for directory in FIRMWARE_FONT_DIRS {
        let directory = Path::new(directory);
        for (regular, bold) in [
            ("LiberationSans.ttf", "LiberationSans-Bold.ttf"),
            ("LiberationSans-Regular.ttf", "LiberationSans-Bold.ttf"),
            ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf"),
        ] {
            let Ok(regular) = fs::read(directory.join(regular)) else {
                continue;
            };
            let faces = fonts.register_fonts(regular.into(), None);
            let Some((family, _)) = faces.first() else {
                continue;
            };
            if let Ok(bold) = fs::read(directory.join(bold)) {
                fonts.register_fonts(bold.into(), None);
            }
            return Some(*family);
        }
    }
    None
}

struct UiFinished(Arc<AtomicBool>);

impl Drop for UiFinished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn schedule_commands(iv: &Inkview) {
    // A timer keeps firmware calls on InkView's event thread, even when a
    // worker requests them while there are no touch or key events.
    unsafe {
        iv.SetHardTimer(
            c"dv-commands".as_ptr(),
            Some(process_commands),
            COMMAND_INTERVAL_MS,
        );
    }
}

extern "C" fn process_commands() {
    let Some((iv, ui_finished, command)) = NATIVE.with(|native| {
        let native = native.borrow();
        let native = native.as_ref()?;
        Some((
            native.iv,
            native.ui_finished.load(Ordering::Acquire),
            native.commands.try_recv().ok(),
        ))
    }) else {
        return;
    };

    if ui_finished {
        unsafe { iv.CloseApp() };
        return;
    }

    // Some firmware versions dismiss the keyboard without calling its handler.
    let keyboard_active = NATIVE.with(|native| {
        native
            .borrow()
            .as_ref()
            .is_some_and(|native| native.keyboard_active)
    });
    if keyboard_active && let Ok(is_open) = iv.IsKeyboardOpened.as_ref() {
        if unsafe { is_open() } == 0 {
            finish_keyboard(None);
        }
    }
    if let Some(command) = command {
        command(iv);
    }
    schedule_commands(iv);
}

fn enqueue(command: Command) -> bool {
    COMMANDS
        .get()
        .is_some_and(|queue| queue.send(command).is_ok())
}

fn request(operation: impl FnOnce(&Inkview) -> bool + Send + 'static) -> bool {
    let (reply, result) = mpsc::channel();
    enqueue(Box::new(move |iv| {
        let _ = reply.send(operation(iv));
    })) && result.recv().unwrap_or(false)
}

/// Opens the native keyboard from the Slint UI thread.
/// `done` runs on the Slint thread after a confirmed edit.
pub fn keyboard(initial: &str, done: impl FnOnce(String) + 'static) {
    keyboard_with_options(c"Поиск", initial, KBD_NORMAL as i32, done);
}

pub fn flibusta_keyboard(initial: &str, password: bool, done: impl FnOnce(String) + 'static) {
    if password {
        keyboard_with_options(c"Пароль Флибусты", "", KBD_PASSWORD as i32, done);
    } else {
        keyboard_with_options(c"Логин Флибусты", initial, KBD_NORMAL as i32, done);
    }
}

fn keyboard_with_options(
    title: &'static CStr,
    initial: &str,
    flags: i32,
    done: impl FnOnce(String) + 'static,
) {
    let pending = KEYBOARD_CALLBACK.with(|callback| {
        if callback.borrow().is_some() {
            return true;
        }
        callback.replace(Some(Box::new(done)));
        false
    });
    if pending {
        return;
    }

    let initial = initial.to_owned();
    if !enqueue(Box::new(move |iv| {
        open_keyboard(iv, title, &initial, flags)
    })) {
        drop(KEYBOARD_CALLBACK.with(|callback| callback.take()));
    }
}

fn open_keyboard(iv: &Inkview, title: &CStr, initial: &str, flags: i32) {
    let Ok(open) = iv.OpenKeyboard.as_ref() else {
        finish_keyboard(None);
        return;
    };

    let Some(buffer) = NATIVE.with(|native| {
        let mut native = native.borrow_mut();
        let native = native.as_mut()?;
        native.keyboard.fill(0);
        let initial = initial.split('\0').next().unwrap_or_default();
        let mut len = initial.len().min(KEYBOARD_LIMIT);
        while !initial.is_char_boundary(len) {
            len -= 1;
        }
        native.keyboard[..len].copy_from_slice(&initial.as_bytes()[..len]);
        native.keyboard_active = true;
        Some(native.keyboard.as_mut_ptr().cast())
    }) else {
        return;
    };

    // The writable buffer lives in the InkView thread state for the duration
    // of the dialog. No Rust borrow of that state is held across this call.
    unsafe {
        open(
            title.as_ptr(),
            buffer,
            KEYBOARD_LIMIT as i32,
            flags,
            Some(keyboard_closed),
        );
    }
}

unsafe extern "C" fn keyboard_closed(text: *mut c_char) {
    let text = if text.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(text) }
                .to_string_lossy()
                .into_owned(),
        )
    };
    finish_keyboard(text);
}

fn finish_keyboard(text: Option<String>) {
    NATIVE.with(|native| {
        let mut native = native.borrow_mut();
        if let Some(native) = native.as_mut() {
            native.keyboard_active = false;
            native.keyboard.fill(0);
            let _ = native.events.send(Event::Callback(Box::new(move || {
                let callback = KEYBOARD_CALLBACK.with(|callback| callback.take());
                if let (Some(callback), Some(text)) = (callback, text) {
                    callback(text);
                }
            })));
        }
    });
}

/// Checks the current connection and asks the firmware to connect if needed.
/// Call from a worker because the firmware can ask the user to select Wi-Fi.
pub fn ensure_network() -> bool {
    request(|iv| {
        if let Ok(query) = iv.QueryNetwork.as_ref() {
            let state = unsafe { query() };
            if state >= 0 && (state as u32 & NET_CONNECTED) != 0 {
                return true;
            }
        }
        iv.NetConnect.as_ref().is_ok_and(|connect| {
            let status = unsafe { connect(std::ptr::null()) };
            status == NET_OK as i32
        })
    })
}

/// Ask the firmware to restore the last connection after a network error.
pub fn reconnect_network() -> bool {
    request(|iv| {
        if let Ok(connect) = iv.NetConnectSilent.as_ref() {
            return unsafe { connect(std::ptr::null()) } == NET_OK as i32;
        }
        if let Ok(connect) = iv.NetConnect3.as_ref() {
            return unsafe { connect(true) };
        }
        iv.NetConnect.as_ref().is_ok_and(|connect| {
            let status = unsafe { connect(std::ptr::null()) };
            status == NET_OK as i32
        })
    })
}

/// Asks the firmware scanner to index Downloads.
pub fn rescan_downloads() -> bool {
    request(|iv| {
        let (Ok(find_task), Ok(send_request)) =
            (iv.FindTaskByAppName.as_ref(), iv.SendRequestTo.as_ref())
        else {
            return false;
        };
        let task = unsafe { find_task(c"scanner.app".as_ptr()) };
        if task < 0 {
            return false;
        }

        let mut scan = *b"-scan:/mnt/ext1/Downloads\0";
        let result = unsafe {
            send_request(
                task,
                REQ_OPENBOOK as i32,
                scan.as_mut_ptr().cast(),
                scan.len() as i32,
                0,
                2000,
            )
        };
        result >= 0
    })
}
