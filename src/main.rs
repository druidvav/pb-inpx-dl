mod device;

use anyhow::{Context, Result, anyhow};
use image::{DynamicImage, ExtendedColorType, ImageEncoder, ImageReader, Limits};
use pb_inpx_dl::CatalogSource;
use pb_inpx_dl::catalog::{Config, Entry, NextPage, Page, SearchMode, is_network_error};
use pb_inpx_dl::cover_cache::DiskCoverCache;
use serde::{Deserialize, Serialize};
use slint::{
    ComponentHandle, Image, Model, Rgba8Pixel, SharedPixelBuffer, Timer, TimerMode, VecModel,
};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{self, Cursor, Write},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};

slint::include_modules!();

const SETTINGS: &str = "/mnt/ext1/system/config/pb-inpx-dl.toml";
const PREFERENCES: &str = "/mnt/ext1/system/config/pb-inpx-dl-preferences.toml";
const DOWNLOADS: &str = "/mnt/ext1/Downloads";
const LOG: &str = "/mnt/ext1/system/pb-inpx-dl.log";
const ROWS: usize = 4;
const COVER_CACHE_SIZE: usize = 48;
const COVER_CACHE_DIR: &str = "/mnt/ext1/system/cache/pb-inpx-dl";
const INPX_SOURCE: usize = 0;
const FLIBUSTA_SOURCE: usize = 1;
const AUTHOR_MODE: i32 = 1;
const BOOK_SCREEN: i32 = 1;
const AUTHOR_CHOOSER_SCREEN: i32 = 2;
const LANGUAGE_SETTINGS_SCREEN: i32 = 3;
const SETTINGS_SCREEN: i32 = 4;
const FLIBUSTA_ACCOUNT_SCREEN: i32 = 5;
const LANGUAGES: &[(&str, &str)] = &[
    ("ru", "Русский"),
    ("en", "Английский"),
    ("uk", "Украинский"),
    ("be", "Белорусский"),
    ("de", "Немецкий"),
    ("fr", "Французский"),
    ("es", "Испанский"),
    ("it", "Итальянский"),
    ("pt", "Португальский"),
    ("pl", "Польский"),
];
const RECONNECTING: &str = "Переподключаем Wi-Fi…";
const WIFI_DISCONNECTED: &str = "Wi-Fi не подключён";
const WIFI_RECONNECT_FAILED: &str = "Не удалось восстановить Wi-Fi";
const CATALOG_UNREACHABLE: &str = "Нет связи с каталогом";

#[derive(Clone)]
enum Target {
    Root,
    Search { term: String, mode: i32 },
    AuthorBooks { name: String, link: Option<String> },
    Feed(String),
    More(NextPage),
}

enum Retry {
    Page { target: Target, reload: bool },
    Download(i32),
}

struct List {
    entries: Vec<Entry>,
    next: Option<NextPage>,
    offset: usize,
    query: String,
    mode: i32,
    target: Target,
    needs_reload: bool,
}

impl List {
    fn new(page: Page, query: String, mode: i32, target: Target) -> Self {
        Self {
            entries: page.entries,
            next: page.next,
            offset: 0,
            query,
            mode,
            target,
            needs_reload: false,
        }
    }

    fn invalidate(&mut self) {
        self.entries.clear();
        self.next = None;
        self.offset = 0;
        self.needs_reload = true;
    }
}

#[derive(Deserialize, Serialize)]
struct Preferences {
    languages: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flibusta: Option<FlibustaAccount>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct FlibustaAccount {
    username: Option<String>,
    password: Option<String>,
}

impl Preferences {
    fn load() -> Result<Option<Self>> {
        match fs::read_to_string(PREFERENCES) {
            Ok(contents) => toml::from_str(&contents)
                .map(Some)
                .map_err(|_| anyhow!("Некорректный файл настроек приложения")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self) -> Result<()> {
        let contents = toml::to_string(self)
            .map_err(|_| anyhow!("Не удалось подготовить файл настроек приложения"))?;
        let path = Path::new(PREFERENCES);
        let directory = path.parent().context("папка настроек")?;
        fs::create_dir_all(directory)?;
        let mut file = tempfile::NamedTempFile::new_in(directory)?;
        file.write_all(contents.as_bytes())?;
        file.as_file().sync_all()?;
        file.persist(path)?;
        Ok(())
    }
}

struct Cover {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
}

impl Cover {
    fn decode(bytes: Vec<u8>, detail: bool) -> Result<Self> {
        let mut reader = ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
        let mut limits = Limits::default();
        limits.max_image_width = Some(4096);
        limits.max_image_height = Some(4096);
        limits.max_alloc = Some(32 * 1024 * 1024);
        reader.limits(limits);
        let pixels = reader.decode()?;
        let size = if detail { (420, 600) } else { (140, 190) };
        let gray = pixels.thumbnail(size.0, size.1).into_luma8();
        let rgba = DynamicImage::ImageLuma8(gray).into_rgba8();
        Ok(Self {
            width: rgba.width(),
            height: rgba.height(),
            rgba: rgba.into_raw(),
        })
    }

    fn png(&self) -> Result<Vec<u8>> {
        let gray: Vec<u8> = self.rgba.chunks_exact(4).map(|pixel| pixel[0]).collect();
        let mut png = Vec::new();
        image::codecs::png::PngEncoder::new(&mut png).write_image(
            &gray,
            self.width,
            self.height,
            ExtendedColorType::L8,
        )?;
        Ok(png)
    }

    fn image(&self) -> Image {
        Image::from_rgba8(SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
            &self.rgba,
            self.width,
            self.height,
        ))
    }
}

struct CoverRequest {
    source: Arc<CatalogSource>,
    generation: u64,
    row: Option<usize>,
    key: String,
    entry: Entry,
}

enum CoverResult {
    Ready(Cover),
    Missing,
    Failed,
}

struct CachedCover {
    image: Option<Image>,
    language: String,
}

fn language_code(language: &str) -> String {
    let language = language.split([',', ';']).next().unwrap_or_default().trim();
    match language.to_ascii_lowercase().as_str() {
        "rus" => "RU".into(),
        "eng" => "EN".into(),
        "ukr" => "UK".into(),
        "deu" | "ger" => "DE".into(),
        "fra" | "fre" => "FR".into(),
        "spa" => "ES".into(),
        code if (2..=12).contains(&code.len())
            && code.bytes().all(|b| b.is_ascii_alphabetic() || b == b'-') =>
        {
            code.to_ascii_uppercase()
        }
        _ => String::new(),
    }
}

fn load_cover(disk: &DiskCoverCache, request: &CoverRequest) -> (String, CoverResult) {
    let detail_view = request.row.is_none();
    let mut language = language_code(&request.entry.language);
    if let Some((cached_language, png)) = disk.get(&request.key) {
        if let Ok(cover) = Cover::decode(png, detail_view) {
            if language.is_empty() {
                language = cached_language;
            }
            return (language, CoverResult::Ready(cover));
        }
        disk.remove(&request.key);
    }

    let entry = &request.entry;
    let url = if detail_view {
        entry.cover.as_ref().or(entry.thumbnail.as_ref())
    } else {
        entry.thumbnail.as_ref().or(entry.cover.as_ref())
    };
    let Some(url) = url else {
        return (language, CoverResult::Missing);
    };
    match request
        .source
        .backend()
        .cover(url)
        .and_then(|bytes| Cover::decode(bytes, detail_view))
    {
        Ok(cover) => {
            if let Ok(png) = cover.png() {
                disk.put(&request.key, &language, &png);
            }
            (language, CoverResult::Ready(cover))
        }
        Err(error)
            if matches!(
                error.downcast_ref::<ureq::Error>(),
                Some(ureq::Error::StatusCode(404))
            ) =>
        {
            (language, CoverResult::Missing)
        }
        Err(_) => (language, CoverResult::Failed),
    }
}

enum Event {
    Loaded(u64, Target, bool, Result<Page>),
    CatalogReconnecting(u64),
    DetailLoaded(u64, Result<Entry>),
    Cover(CoverRequest, String, CoverResult),
    Progress(u64, Option<u64>),
    DownloadStatus(&'static str),
    Downloaded(i32, String, Result<(PathBuf, bool)>),
}

type CatalogSources = [Option<Result<Arc<CatalogSource>, String>>; 2];

fn catalog_sources(config: &Config) -> CatalogSources {
    let inpx = config.inpx_enabled().then(|| {
        CatalogSource::inpx(config.clone())
            .map(Arc::new)
            .map_err(|error| format!("Настройки INPX: {error}"))
    });
    let flibusta = CatalogSource::flibusta(config.clone())
        .map(Arc::new)
        .map_err(|error| format!("Настройки Флибусты: {error}"));
    [inpx, Some(flibusta)]
}

struct App {
    window: slint::Weak<CatalogWindow>,
    config: Config,
    account_override: Option<FlibustaAccount>,
    draft_account: FlibustaAccount,
    language_codes: Vec<String>,
    draft_languages: Vec<String>,
    source: usize,
    sources: CatalogSources,
    send: Sender<Event>,
    cover_queue: Sender<CoverRequest>,
    generation: Arc<AtomicU64>,
    request_id: Cell<u64>,
    retry: Option<Retry>,
    current: List,
    history: Vec<List>,
    detail: Option<Entry>,
    downloaded: HashSet<String>,
    rows: Rc<VecModel<Row>>,
    covers: HashMap<String, CachedCover>,
    cover_order: VecDeque<String>,
}

fn log_error(error: &anyhow::Error) {
    log_message(&format!("{error:#}"));
}

fn log_message(message: &str) {
    eprintln!("{message}");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG)
    {
        let _ = writeln!(file, "{message}");
    }
}

fn with_network_retry<T>(
    mut operation: impl FnMut() -> Result<T>,
    reconnecting: impl FnOnce(),
) -> Result<T> {
    if !device::ensure_network() {
        return Err(anyhow!(WIFI_DISCONNECTED));
    }
    let error = match operation() {
        Err(error) if is_network_error(&error) => error,
        result => return result,
    };

    reconnecting();
    if !device::reconnect_network() {
        return Err(error.context(WIFI_RECONNECT_FAILED));
    }
    match operation() {
        Err(error) if is_network_error(&error) => Err(error.context(CATALOG_UNREACHABLE)),
        result => result,
    }
}

fn request_error_message(error: &anyhow::Error, fallback: &'static str) -> &'static str {
    match error.to_string().as_str() {
        WIFI_DISCONNECTED => WIFI_DISCONNECTED,
        WIFI_RECONNECT_FAILED => WIFI_RECONNECT_FAILED,
        CATALOG_UNREACHABLE => CATALOG_UNREACHABLE,
        _ => fallback,
    }
}

fn search_mode(mode: i32) -> SearchMode {
    match mode {
        AUTHOR_MODE => SearchMode::Authors,
        _ => SearchMode::Books,
    }
}

fn cover_key(entry: &Entry, large: bool, source: &CatalogSource) -> Option<String> {
    let url = entry
        .detail
        .as_ref()
        .or(entry.thumbnail.as_ref())
        .or(entry.cover.as_ref())?;
    if source.key == "inpx" {
        Some(format!("{large}:{url}"))
    } else {
        Some(format!("{}:{large}:{url}", source.key))
    }
}

impl App {
    fn client(&self) -> Option<&Arc<CatalogSource>> {
        self.sources[self.source].as_ref()?.as_ref().ok()
    }

    fn show_catalog_message(&mut self, ui: &CatalogWindow, message: &str, retry: Option<Retry>) {
        ui.set_catalog_can_retry(retry.is_some());
        ui.set_catalog_message(message.into());
        self.retry = retry;
    }

    fn dismiss_catalog_message(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if !ui.get_busy() {
            if matches!(
                self.retry,
                Some(Retry::Page {
                    target: Target::Search { .. },
                    ..
                })
            ) {
                ui.set_query(self.current.query.as_str().into());
            }
            self.show_catalog_message(&ui, "", None);
        }
    }

    fn retry_catalog_request(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || ui.get_downloading() {
            return;
        }
        if let Some(retry) = self.retry.take() {
            self.show_catalog_message(&ui, "", None);
            match retry {
                Retry::Page { target, reload } => self.request_page(target, reload),
                Retry::Download(index) => self.download(index),
            }
        }
    }

    fn request(&mut self, target: Target) {
        self.request_page(target, false);
    }

    fn request_page(&mut self, target: Target, reload: bool) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || ui.get_downloading() {
            return;
        }
        let Some(client) = self.sources[self.source].as_ref() else {
            return;
        };
        let client = match client.clone() {
            Ok(client) => client,
            Err(error) => {
                log_message(&error);
                self.show_catalog_message(&ui, &error, None);
                return;
            }
        };
        let request_id = self.request_id.get() + 1;
        self.request_id.set(request_id);
        if let Target::Search { term, .. } = &target {
            ui.set_query(term.as_str().into());
        }
        ui.set_busy(true);
        self.show_catalog_message(&ui, "Загрузка…", None);
        let send = self.send.clone();
        thread::spawn(move || {
            let backend = client.backend();
            let load = || {
                if reload
                    && client.key == "flibusta"
                    && matches!(
                        &target,
                        Target::AuthorBooks { link: Some(_), .. } | Target::Feed(_)
                    )
                {
                    // Restore the account session before reloading an existing feed.
                    backend.root()?;
                }
                match &target {
                    Target::Root => backend.root(),
                    Target::Search { term, mode } => backend.search(search_mode(*mode), term),
                    Target::AuthorBooks {
                        link: Some(link), ..
                    }
                    | Target::Feed(link) => backend.page(link),
                    Target::AuthorBooks { name, link: None } => backend.books_by_author(name),
                    Target::More(next) => backend.more(next),
                }
            };
            let result = if matches!(&target, Target::Root) && client.key == "inpx" {
                load()
            } else {
                with_network_retry(load, || {
                    let _ = send.send(Event::CatalogReconnecting(request_id));
                })
            };
            let _ = send.send(Event::Loaded(request_id, target, reload, result));
        });
    }

    fn select_source(&mut self, source: i32) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || ui.get_downloading() {
            return;
        }
        let Ok(index) = usize::try_from(source) else {
            return;
        };
        if index == self.source {
            return;
        }
        let Some(Some(client)) = self.sources.get(index) else {
            return;
        };
        if let Err(error) = client {
            let error = error.clone();
            log_message(&error);
            self.show_catalog_message(&ui, &error, None);
            return;
        }
        self.source = index;
        ui.set_source(source);
        self.history.clear();
        self.current = List::new(Page::default(), String::new(), 0, Target::Root);
        self.render_list();
        self.request(Target::Root);
    }

    fn open_settings(&self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() == 0 && !ui.get_busy() && !ui.get_downloading() {
            ui.set_screen(SETTINGS_SCREEN);
        }
    }

    fn open_language_settings(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != SETTINGS_SCREEN {
            return;
        }
        self.draft_languages.clone_from(&self.config.languages);
        self.language_codes = LANGUAGES.iter().map(|(code, _)| (*code).into()).collect();
        for code in &self.draft_languages {
            if !self
                .language_codes
                .iter()
                .any(|known| known.eq_ignore_ascii_case(code))
            {
                self.language_codes.push(code.clone());
            }
        }
        ui.set_language_status("".into());
        self.render_language_settings();
        ui.set_screen(LANGUAGE_SETTINGS_SCREEN);
    }

    fn render_language_settings(&self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        let options: Vec<_> = self
            .language_codes
            .iter()
            .map(|code| {
                let label = LANGUAGES
                    .iter()
                    .find(|(known, _)| code.eq_ignore_ascii_case(known))
                    .map_or(code.as_str(), |(_, name)| *name);
                LanguageOption {
                    label: label.into(),
                    selected: self
                        .draft_languages
                        .iter()
                        .any(|value| value.eq_ignore_ascii_case(code)),
                }
            })
            .collect();
        ui.set_language_options(Rc::new(VecModel::from(options)).into());
        ui.set_all_languages(self.draft_languages.is_empty());
    }

    fn toggle_language(&mut self, index: i32) {
        if index == -1 {
            self.draft_languages.clear();
        } else {
            let Some(code) = usize::try_from(index)
                .ok()
                .and_then(|index| self.language_codes.get(index))
            else {
                return;
            };
            let count = self.draft_languages.len();
            self.draft_languages
                .retain(|value| !value.eq_ignore_ascii_case(code));
            if self.draft_languages.len() == count {
                self.draft_languages.push(code.clone());
            }
        }
        self.render_language_settings();
    }

    fn add_language(&mut self, input: String) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != LANGUAGE_SETTINGS_SCREEN {
            return;
        }
        let code = input.trim().to_ascii_lowercase();
        if !(2..=3).contains(&code.len()) || !code.bytes().all(|byte| byte.is_ascii_lowercase()) {
            ui.set_language_status("Введите код из 2–3 латинских букв".into());
            return;
        }
        if !self
            .language_codes
            .iter()
            .any(|known| known.eq_ignore_ascii_case(&code))
        {
            self.language_codes.insert(LANGUAGES.len(), code.clone());
        }
        if !self
            .draft_languages
            .iter()
            .any(|value| value.eq_ignore_ascii_case(&code))
        {
            self.draft_languages.push(code);
        }
        ui.set_language_status("".into());
        self.render_language_settings();
    }

    fn apply_languages(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != LANGUAGE_SETTINGS_SCREEN {
            return;
        }
        if self.draft_languages == self.config.languages {
            ui.set_screen(0);
            return;
        }

        if let Err(error) = self.save_languages() {
            log_error(&error);
            ui.set_language_status("Не удалось сохранить настройки".into());
            return;
        }
        self.current.invalidate();
        for list in &mut self.history {
            list.invalidate();
        }
        self.render_list();
        self.request_page(self.current.target.clone(), true);
    }

    fn save_languages(&mut self) -> Result<()> {
        let mut config = self.config.clone();
        config.languages.clone_from(&self.draft_languages);
        let sources = catalog_sources(&config);
        Preferences {
            languages: config.languages.clone(),
            flibusta: self.account_override.clone(),
        }
        .save()?;

        self.config = config;
        self.sources = sources;
        Ok(())
    }

    fn open_flibusta_account(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != SETTINGS_SCREEN {
            return;
        }
        self.draft_account = FlibustaAccount {
            username: self.config.flibusta.username.clone(),
            password: self.config.flibusta.password.clone(),
        };
        ui.set_flibusta_can_logout(
            self.config.flibusta.username.is_some() || self.config.flibusta.password.is_some(),
        );
        self.render_flibusta_account(&ui);
        ui.set_screen(FLIBUSTA_ACCOUNT_SCREEN);
    }

    fn render_flibusta_account(&self, ui: &CatalogWindow) {
        ui.set_flibusta_username(self.draft_account.username.as_deref().unwrap_or("").into());
        ui.set_flibusta_has_password(
            self.draft_account
                .password
                .as_ref()
                .is_some_and(|password| !password.is_empty()),
        );
        ui.set_flibusta_account_status("".into());
    }

    fn edit_flibusta_account(&mut self, input: String, password: bool) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != FLIBUSTA_ACCOUNT_SCREEN {
            return;
        }
        if password {
            self.draft_account.password = Some(input);
        } else {
            self.draft_account.username = Some(input.trim().to_owned());
        }
        self.render_flibusta_account(&ui);
    }

    fn apply_flibusta_account(&mut self, logout: bool) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_screen() != FLIBUSTA_ACCOUNT_SCREEN {
            return;
        }
        let account = if logout {
            FlibustaAccount::default()
        } else {
            match (&self.draft_account.username, &self.draft_account.password) {
                (Some(username), Some(password))
                    if !username.is_empty() && !password.is_empty() =>
                {
                    if username.contains(':') {
                        ui.set_flibusta_account_status("Логин не может содержать двоеточие".into());
                        return;
                    }
                }
                _ => {
                    ui.set_flibusta_account_status("Укажи логин и пароль".into());
                    return;
                }
            }
            self.draft_account.clone()
        };

        if account.username == self.config.flibusta.username
            && account.password == self.config.flibusta.password
        {
            self.draft_account = FlibustaAccount::default();
            ui.set_screen(0);
            return;
        }

        let mut config = self.config.clone();
        config.flibusta.username.clone_from(&account.username);
        config.flibusta.password.clone_from(&account.password);
        let client = match CatalogSource::flibusta(config.clone()) {
            Ok(client) => Arc::new(client),
            Err(error) => {
                log_error(&error);
                ui.set_flibusta_account_status("Проверь настройки каталога Флибусты".into());
                return;
            }
        };
        let preferences = Preferences {
            languages: config.languages.clone(),
            flibusta: Some(account),
        };
        if let Err(error) = preferences.save() {
            log_error(&error);
            ui.set_flibusta_account_status("Не удалось сохранить настройки".into());
            return;
        }

        self.config = config;
        self.account_override = preferences.flibusta;
        self.draft_account = FlibustaAccount::default();
        self.sources[FLIBUSTA_SOURCE] = Some(Ok(client));
        if self.source == FLIBUSTA_SOURCE {
            self.history.clear();
            self.current = List::new(Page::default(), String::new(), 0, Target::Root);
            self.render_list();
            self.request(Target::Root);
        } else {
            ui.set_screen(0);
        }
    }

    fn render_list(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        self.detail = None;
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let list = &self.current;
        let visible = list.entries.iter().skip(list.offset).take(ROWS);
        let client = self.client();
        let mut rows = Vec::new();
        for (row, entry) in visible.enumerate() {
            let key = client.and_then(|client| cover_key(entry, false, client));
            let cached = key.as_ref().and_then(|key| self.covers.get(key));
            let mut language = language_code(&entry.language);
            let cover = cached.and_then(|cover| cover.image.clone());
            if language.is_empty() {
                if let Some(cached) = cached {
                    language.clone_from(&cached.language);
                }
            }
            let cover_state = if cover.is_some() {
                2
            } else if cached.is_none()
                && let Some(key) = key
                && let Some(client) = client
            {
                let _ = self.cover_queue.send(CoverRequest {
                    source: client.clone(),
                    generation,
                    row: Some(row),
                    key,
                    entry: entry.clone(),
                });
                1
            } else {
                0
            };
            rows.push(Row {
                title: entry.title.as_str().into(),
                subtitle: entry.author.as_str().into(),
                language: language.into(),
                series: entry.series.as_str().into(),
                cover: cover.clone().unwrap_or_default(),
                has_cover: cover.is_some(),
                cover_state,
                is_book: entry.navigation.is_none(),
            });
        }
        self.rows.set_vec(rows);
        ui.set_screen(0);
        ui.set_query(list.query.as_str().into());
        ui.set_mode(list.mode);
        ui.set_can_back(!self.history.is_empty());
        ui.set_can_prev(list.offset > 0);
        ui.set_can_next(list.offset + ROWS < list.entries.len() || list.next.is_some());
        let page = if list.entries.is_empty() {
            String::new()
        } else {
            format!("Стр. {}", list.offset / ROWS + 1)
        };
        ui.set_page_label(page.into());
    }

    fn render_detail(&mut self, entry: Entry) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        let Some(client) = self.client().cloned() else {
            return;
        };
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let key = cover_key(&entry, true, &client);
        let cached = key.as_ref().and_then(|key| self.covers.get(key));
        let cover = cached.and_then(|cached| cached.image.clone());
        if let Some(key) = key.filter(|_| cached.is_none()) {
            let _ = self.cover_queue.send(CoverRequest {
                source: client.clone(),
                generation,
                row: None,
                key,
                entry: entry.clone(),
            });
        }
        let preview = cover.or_else(|| {
            cover_key(&entry, false, &client).and_then(|key| self.covers.get(&key)?.image.clone())
        });
        ui.set_detail_cover(preview.clone().unwrap_or_default());
        ui.set_detail_has_cover(preview.is_some());
        ui.set_detail_title(entry.title.as_str().into());
        ui.set_detail_author(entry.author.as_str().into());
        let mut authors = Vec::new();
        let byline = entry.author.split(',').map(str::trim);
        let author_links = entry.author_links.iter().map(|(name, _)| name.as_str());
        for author in author_links.chain(byline.filter(|_| entry.author_links.is_empty())) {
            if !author.is_empty() && !authors.contains(&author) {
                authors.push(author);
            }
        }
        ui.set_author_choices(
            Rc::new(VecModel::from(
                authors
                    .into_iter()
                    .map(slint::SharedString::from)
                    .collect::<Vec<_>>(),
            ))
            .into(),
        );
        ui.set_detail_description(entry.description.as_str().into());
        ui.set_detail_meta(entry.metadata.as_str().into());
        ui.set_detail_extra_metadata(entry.extra_metadata.as_str().into());
        ui.set_detail_statistics(entry.statistics.as_str().into());
        ui.set_formats(
            Rc::new(VecModel::from(
                entry
                    .downloads
                    .iter()
                    .map(|d| d.label.as_str().into())
                    .collect::<Vec<_>>(),
            ))
            .into(),
        );
        ui.set_downloaded(
            entry
                .downloads
                .first()
                .is_some_and(|download| self.downloaded.contains(&download.url)),
        );
        ui.set_status("".into());
        ui.set_progress("".into());
        ui.set_detail_error("".into());
        ui.set_detail_loading(entry.detail.is_some());
        ui.set_screen(BOOK_SCREEN);
        ui.set_can_back(true);
        self.detail = Some(entry.clone());

        if ui.get_detail_loading() {
            let send = self.send.clone();
            thread::spawn(move || {
                let started = Instant::now();
                let connected = device::ensure_network();
                let network_check = started.elapsed();
                let result = if connected {
                    client.backend().detail(&entry)
                } else {
                    Err(anyhow!("Wi-Fi не подключён"))
                };
                let elapsed = started.elapsed();
                let success = result.is_ok();
                let _ = send.send(Event::DetailLoaded(generation, result));
                if elapsed >= Duration::from_secs(2) {
                    log_message(&format!(
                        "Annotation: {} ms Wi-Fi check, {} ms {}, success={success}",
                        network_check.as_millis(),
                        (elapsed - network_check).as_millis(),
                        client.key,
                    ));
                }
            });
        }
    }

    fn accept(&mut self, event: Event) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        match event {
            Event::CatalogReconnecting(request_id) => {
                if request_id == self.request_id.get() {
                    ui.set_catalog_message(RECONNECTING.into());
                }
            }
            Event::Loaded(request_id, target, reload, result) => {
                if request_id != self.request_id.get() {
                    return;
                }
                ui.set_busy(false);
                let page = match result {
                    Ok(page) => page,
                    Err(error) => {
                        log_error(&error);
                        self.show_catalog_message(
                            &ui,
                            request_error_message(&error, "Не удалось загрузить каталог."),
                            Some(Retry::Page { target, reload }),
                        );
                        return;
                    }
                };
                self.show_catalog_message(&ui, "", None);
                let target_for_list = target.clone();
                if reload {
                    self.current.entries = page.entries;
                    self.current.next = page.next;
                    self.current.needs_reload = false;
                    self.render_list();
                    return;
                }
                match target {
                    Target::Root => {
                        self.history.clear();
                        self.current = List::new(page, String::new(), 0, target_for_list);
                        self.render_list();
                    }
                    Target::Search { term, mode } => {
                        let same_query = self.current.query == term;
                        let previous = std::mem::replace(
                            &mut self.current,
                            List::new(page, term, mode, target_for_list),
                        );
                        if !same_query || previous.query.is_empty() {
                            self.history.push(previous);
                        }
                        self.render_list();
                    }
                    Target::AuthorBooks { name, .. } => {
                        self.current = List::new(page, name, AUTHOR_MODE, target_for_list);
                        self.render_list();
                    }
                    Target::Feed(_) => {
                        let view = List::new(
                            page,
                            self.current.query.clone(),
                            self.current.mode,
                            target_for_list,
                        );
                        self.history
                            .push(std::mem::replace(&mut self.current, view));
                        self.render_list();
                    }
                    Target::More(url) => {
                        let can_advance = !page.entries.is_empty()
                            && self.current.offset + ROWS <= self.current.entries.len();
                        self.current.entries.extend(page.entries);
                        self.current.next = page.next.filter(|next| next != &url);
                        if can_advance {
                            self.current.offset += ROWS;
                        }
                        self.render_list();
                    }
                }
            }
            Event::DetailLoaded(generation, result) => {
                if generation != self.generation.load(Ordering::Relaxed) {
                    return;
                }
                ui.set_detail_loading(false);
                match result {
                    Ok(entry) => {
                        ui.set_detail_description(entry.description.as_str().into());
                        ui.set_detail_meta(entry.metadata.as_str().into());
                        ui.set_detail_extra_metadata(entry.extra_metadata.as_str().into());
                        ui.set_detail_statistics(entry.statistics.as_str().into());
                    }
                    Err(error) => {
                        log_error(&error);
                        ui.set_detail_error("Не удалось загрузить аннотацию".into());
                    }
                }
            }
            Event::Cover(request, language, result) => {
                if request.generation != self.generation.load(Ordering::Relaxed) {
                    return;
                }
                let (state, image) = match result {
                    CoverResult::Ready(cover) => (2, Some(cover.image())),
                    CoverResult::Missing => (0, None),
                    CoverResult::Failed => (3, None),
                };
                if state != 3 && !self.covers.contains_key(&request.key) {
                    if self.cover_order.len() >= COVER_CACHE_SIZE
                        && let Some(old) = self.cover_order.pop_front()
                    {
                        self.covers.remove(&old);
                    }
                    self.cover_order.push_back(request.key.clone());
                    self.covers.insert(
                        request.key,
                        CachedCover {
                            image: image.clone(),
                            language: language.clone(),
                        },
                    );
                }
                if let Some(index) = request.row {
                    if let Some(mut row) = self.rows.row_data(index) {
                        row.cover = image.clone().unwrap_or_default();
                        row.has_cover = image.is_some();
                        row.cover_state = state;
                        row.language = language.into();
                        self.rows.set_row_data(index, row);
                    }
                } else if let Some(image) = image {
                    ui.set_detail_cover(image);
                    ui.set_detail_has_cover(true);
                }
            }
            Event::Progress(bytes, total) => {
                let text = match total.filter(|n| *n > 0) {
                    Some(total) => format!("{}%", (bytes.saturating_mul(100) / total).min(100)),
                    None => format!("{} КБ", bytes / 1024),
                };
                ui.set_progress(text.into());
            }
            Event::DownloadStatus(status) => {
                ui.set_progress(status.into());
            }
            Event::Downloaded(index, url, result) => {
                ui.set_downloading(false);
                ui.set_progress("".into());
                match result {
                    Ok((path, indexed)) => {
                        self.downloaded.insert(url);
                        ui.set_downloaded(true);
                        if !indexed {
                            let name = path.file_name().unwrap_or_default().to_string_lossy();
                            let text = format!(
                                "Сохранено в Downloads: {name}. Обнови библиотеку в меню PocketBook."
                            );
                            ui.set_status(text.into());
                        }
                    }
                    Err(error) => {
                        log_error(&error);
                        self.show_catalog_message(
                            &ui,
                            request_error_message(&error, "Скачать книгу не удалось."),
                            Some(Retry::Download(index)),
                        );
                    }
                }
            }
        }
    }

    fn open(&mut self, row: i32) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || row < 0 {
            return;
        }
        let Some(entry) = self
            .current
            .entries
            .get(self.current.offset + row as usize)
            .cloned()
        else {
            return;
        };
        if let Some(url) = entry.navigation {
            self.request(Target::Feed(url));
        } else {
            self.render_detail(entry);
        }
    }

    fn back(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_downloading() {
            return;
        }
        if !ui.get_catalog_message().is_empty() {
            return;
        }
        if ui.get_screen() == FLIBUSTA_ACCOUNT_SCREEN {
            self.draft_account = FlibustaAccount::default();
            ui.set_screen(SETTINGS_SCREEN);
        } else if ui.get_screen() == LANGUAGE_SETTINGS_SCREEN {
            ui.set_screen(SETTINGS_SCREEN);
        } else if ui.get_screen() == SETTINGS_SCREEN {
            ui.set_screen(0);
        } else if ui.get_screen() == AUTHOR_CHOOSER_SCREEN {
            ui.set_screen(BOOK_SCREEN);
        } else if self.detail.is_some() {
            self.request_id.set(self.request_id.get() + 1);
            ui.set_busy(false);
            self.render_list();
        } else if ui.get_busy() {
            return;
        } else if let Some(previous) = self.history.pop() {
            self.current = previous;
            self.render_list();
            if self.current.needs_reload {
                self.request_page(self.current.target.clone(), true);
            }
        } else {
            let _ = slint::quit_event_loop();
        }
    }

    fn turn(&mut self, next: bool) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || self.detail.is_some() {
            return;
        }
        if !next {
            self.current.offset = self.current.offset.saturating_sub(ROWS);
            self.render_list();
        } else if self.current.offset + ROWS < self.current.entries.len() {
            self.current.offset += ROWS;
            self.render_list();
        } else if let Some(url) = self.current.next.clone() {
            self.request(Target::More(url));
        }
    }

    fn search(&mut self, term: String, mode: i32) {
        let term = term.trim().to_owned();
        if term.is_empty() {
            return;
        }
        self.request(Target::Search { term, mode });
    }

    fn open_author_books(&mut self) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || ui.get_downloading() {
            return;
        }
        match ui.get_author_choices().row_count() {
            0 => {}
            1 => self.select_author(0),
            _ => ui.set_screen(AUTHOR_CHOOSER_SCREEN),
        }
    }

    fn select_author(&mut self, index: i32) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if self.detail.is_none() || ui.get_busy() || ui.get_downloading() {
            return;
        }
        let Some(author) = usize::try_from(index)
            .ok()
            .and_then(|index| ui.get_author_choices().row_data(index))
        else {
            return;
        };
        let author = author.to_string();
        let link = self.detail.as_ref().and_then(|entry| {
            entry
                .author_links
                .iter()
                .find(|(name, link)| name == &author && !link.is_empty())
                .map(|(_, link)| link.clone())
        });
        let target = Target::AuthorBooks {
            name: author.clone(),
            link,
        };
        let previous = std::mem::replace(
            &mut self.current,
            List::new(Page::default(), author, AUTHOR_MODE, target.clone()),
        );
        self.history.push(previous);
        self.render_list();
        self.request(target);
    }

    fn download(&mut self, index: i32) {
        let Some(ui) = self.window.upgrade() else {
            return;
        };
        if ui.get_busy() || ui.get_downloading() || index < 0 {
            return;
        }
        let Some(entry) = self.detail.clone() else {
            return;
        };
        let Some(download) = entry.downloads.get(index as usize).cloned() else {
            return;
        };
        if self.downloaded.contains(&download.url) {
            return;
        }
        let Some(client) = self.client().cloned() else {
            return;
        };
        ui.set_downloading(true);
        ui.set_status("".into());
        ui.set_progress("Готовим…".into());
        let send = self.send.clone();
        thread::spawn(move || {
            let result = with_network_retry(
                || {
                    let _ = send.send(Event::DownloadStatus("Скачиваем…"));
                    let mut last_progress = Instant::now();
                    client.backend().download(
                        &download,
                        &entry,
                        Path::new(DOWNLOADS),
                        &mut |bytes, total| {
                            if last_progress.elapsed() >= Duration::from_millis(350) {
                                let _ = send.send(Event::Progress(bytes, total));
                                last_progress = Instant::now();
                            }
                        },
                    )
                },
                || {
                    let _ = send.send(Event::DownloadStatus(RECONNECTING));
                },
            )
            .map(|path| {
                let indexed = device::rescan_downloads();
                (path, indexed)
            });
            let _ = send.send(Event::Downloaded(index, download.url, result));
        });
    }
}

fn run_app() -> Result<()> {
    let window = CatalogWindow::new().context("создание окна")?;
    let mut config = Config::load(Path::new(SETTINGS))?;
    let mut account_override = None;
    match Preferences::load() {
        Ok(Some(preferences)) => {
            config.languages = preferences.languages;
            account_override = preferences.flibusta;
            if let Some(account) = &account_override {
                config.flibusta.username.clone_from(&account.username);
                config.flibusta.password.clone_from(&account.password);
            }
        }
        Ok(None) => {}
        Err(error) => log_error(&error.context("чтение настроек приложения")),
    }
    let sources = catalog_sources(&config);
    let inpx_enabled = sources[INPX_SOURCE].is_some();
    let source = if inpx_enabled {
        INPX_SOURCE
    } else {
        FLIBUSTA_SOURCE
    };
    window.set_inpx_enabled(inpx_enabled);
    window.set_source(source as i32);
    let generation = Arc::new(AtomicU64::new(0));
    let rows = Rc::new(VecModel::default());
    window.set_rows(rows.clone().into());
    let (send, receive) = mpsc::channel();
    let (cover_queue, cover_requests) = mpsc::channel::<CoverRequest>();
    {
        let send = send.clone();
        let generation = generation.clone();
        thread::spawn(move || {
            let disk = DiskCoverCache::new(COVER_CACHE_DIR);
            while let Ok(request) = cover_requests.recv() {
                if request.generation != generation.load(Ordering::Relaxed) {
                    continue;
                }
                let (language, cover) = load_cover(&disk, &request);
                if send.send(Event::Cover(request, language, cover)).is_err() {
                    break;
                }
            }
        });
    }
    let app = Rc::new(RefCell::new(App {
        window: window.as_weak(),
        config,
        account_override,
        draft_account: FlibustaAccount::default(),
        language_codes: Vec::new(),
        draft_languages: Vec::new(),
        source,
        sources,
        send,
        cover_queue,
        generation,
        request_id: Cell::new(0),
        retry: None,
        current: List::new(
            Page {
                entries: Vec::new(),
                next: None,
            },
            String::new(),
            0,
            Target::Root,
        ),
        history: Vec::new(),
        detail: None,
        downloaded: HashSet::new(),
        rows,
        covers: HashMap::new(),
        cover_order: VecDeque::new(),
    }));
    let timer = Timer::default();
    {
        let app = Rc::downgrade(&app);
        timer.start(TimerMode::Repeated, Duration::from_millis(120), move || {
            if let Some(app) = app.upgrade() {
                for event in receive.try_iter().take(12) {
                    app.borrow_mut().accept(event);
                }
            }
        });
    }
    {
        let app = app.clone();
        window.on_open_search(move || {
            let state = app.borrow();
            let Some(ui) = state.window.upgrade() else {
                return;
            };
            if ui.get_busy() || ui.get_downloading() {
                return;
            }
            let query = ui.get_query();
            let mode = ui.get_mode();
            drop(state);
            let app = app.clone();
            device::keyboard(query.as_str(), move |term| {
                app.borrow_mut().search(term, mode)
            });
        });
    }
    {
        let app = app.clone();
        window.on_change_mode(move |mode| {
            let mut state = app.borrow_mut();
            let Some(ui) = state.window.upgrade() else {
                return;
            };
            if ui.get_busy() {
                return;
            }
            let query = ui.get_query();
            if query.is_empty() {
                ui.set_mode(mode);
                state.current.mode = mode;
            } else {
                state.search(query.to_string(), mode);
            }
        });
    }
    {
        let app = app.clone();
        window.on_select_source(move |source| app.borrow_mut().select_source(source));
    }
    {
        let app = app.clone();
        window.on_go_back(move || app.borrow_mut().back());
    }
    {
        let app = app.clone();
        window.on_dismiss_catalog_message(move || app.borrow_mut().dismiss_catalog_message());
    }
    {
        let app = app.clone();
        window.on_retry_catalog_request(move || app.borrow_mut().retry_catalog_request());
    }
    {
        let app = app.clone();
        window.on_open_settings(move || app.borrow().open_settings());
    }
    {
        let app = app.clone();
        window.on_open_flibusta_account(move || app.borrow_mut().open_flibusta_account());
    }
    {
        let app = app.clone();
        window.on_edit_flibusta_account(move |password| {
            let state = app.borrow();
            let initial = if password {
                String::new()
            } else {
                state.draft_account.username.clone().unwrap_or_default()
            };
            drop(state);
            let app = app.clone();
            device::flibusta_keyboard(&initial, password, move |input| {
                app.borrow_mut().edit_flibusta_account(input, password);
            });
        });
    }
    {
        let app = app.clone();
        window.on_apply_flibusta_account(move |logout| {
            app.borrow_mut().apply_flibusta_account(logout)
        });
    }
    {
        let app = app.clone();
        window.on_open_language_settings(move || app.borrow_mut().open_language_settings());
    }
    {
        let app = app.clone();
        window.on_toggle_language(move |index| app.borrow_mut().toggle_language(index));
    }
    {
        let app = app.clone();
        window.on_add_language(move || {
            let app = app.clone();
            device::keyboard("", move |input| app.borrow_mut().add_language(input));
        });
    }
    {
        let app = app.clone();
        window.on_apply_languages(move || app.borrow_mut().apply_languages());
    }
    {
        let app = app.clone();
        window.on_previous_page(move || app.borrow_mut().turn(false));
    }
    {
        let app = app.clone();
        window.on_next_page(move || app.borrow_mut().turn(true));
    }
    {
        let app = app.clone();
        window.on_open_row(move |row| app.borrow_mut().open(row));
    }
    {
        let app = app.clone();
        window.on_open_author_books(move || app.borrow_mut().open_author_books());
    }
    {
        let app = app.clone();
        window.on_select_author(move |index| app.borrow_mut().select_author(index));
    }
    {
        let app = app.clone();
        window.on_download_format(move |index| app.borrow_mut().download(index));
    }
    app.borrow_mut().request(Target::Root);
    window.run().context("цикл интерфейса PocketBook")
}

fn main() {
    if let Err(error) = device::run(run_app) {
        log_error(&error);
        std::process::exit(1);
    }
}
