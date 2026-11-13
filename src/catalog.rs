use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tempfile::Builder;
use tungstenite::{
    HandshakeError, Message, client::IntoClientRequest, client_tls_with_config,
    error::ProtocolError, http::header::ORIGIN,
};
use ureq::unversioned::{
    resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver},
    transport::{DefaultConnector, NextTimeout},
};
use url::Url;

const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const COVER_LIMIT: usize = 8 * 1024 * 1024;
const DETAIL_CACHE_CAPACITY: usize = 16;
const INPX_PAGE_SIZE: usize = 40;
const FILTERED_PAGE_LOOKAHEAD: usize = 8;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug)]
struct NetworkFailure(&'static str);

impl std::fmt::Display for NetworkFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for NetworkFailure {}

pub fn is_network_error(error: &anyhow::Error) -> bool {
    error.is::<NetworkFailure>()
        || error.chain().any(|cause| {
            if let Some(error) = cause.downcast_ref::<ureq::Error>() {
                return match error {
                    ureq::Error::HostNotFound
                    | ureq::Error::Timeout(_)
                    | ureq::Error::ConnectionFailed => true,
                    ureq::Error::Io(error) => is_transport_io(error),
                    _ => false,
                };
            }
            match cause.downcast_ref::<tungstenite::Error>() {
                Some(tungstenite::Error::Io(error)) => is_transport_io(error),
                Some(
                    tungstenite::Error::ConnectionClosed
                    | tungstenite::Error::Protocol(
                        ProtocolError::HandshakeIncomplete
                        | ProtocolError::ResetWithoutClosingHandshake,
                    ),
                ) => true,
                _ => false,
            }
        })
}

fn is_transport_io(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
    )
}

#[derive(Debug, Default)]
struct CatalogResolver(DefaultResolver);

impl Resolver for CatalogResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        // getaddrinfo failures can arrive as generic I/O errors.
        self.0
            .resolve(uri, config, timeout)
            .map_err(|error| match error {
                ureq::Error::Io(_) => ureq::Error::HostNotFound,
                error => error,
            })
    }
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub inpx_ws_url: Option<String>,
    pub inpx_http_url: Option<String>,
    pub flibusta: FlibustaConfig,
    pub languages: Vec<String>,
    catalog_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            inpx_ws_url: None,
            inpx_http_url: None,
            flibusta: FlibustaConfig::default(),
            languages: vec!["en".into(), "ru".into()],
            catalog_url: None,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct FlibustaConfig {
    pub opds_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for FlibustaConfig {
    fn default() -> Self {
        Self {
            opds_url: "https://flibusta.is/opds".into(),
            username: None,
            password: None,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        match fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text).map_err(|error| {
                // TOML's diagnostic includes the input line, which may contain a password.
                let line = error.span().map(|span| {
                    1 + text
                        .bytes()
                        .take(span.start)
                        .filter(|byte| *byte == b'\n')
                        .count()
                });
                match line {
                    Some(line) => {
                        anyhow!("Invalid configuration in {} at line {line}", path.display())
                    }
                    None => anyhow!("Invalid configuration in {}", path.display()),
                }
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error).with_context(|| format!("Reading {}", path.display())),
        }
    }

    pub fn inpx_enabled(&self) -> bool {
        [&self.inpx_ws_url, &self.inpx_http_url, &self.catalog_url]
            .into_iter()
            .any(|address| configured_address(address).is_some())
    }

    fn endpoints(&self) -> Result<(Url, Url)> {
        let websocket = configured_address(&self.inpx_ws_url);
        let http = configured_address(&self.inpx_http_url);
        let legacy_base = if websocket.is_none() && http.is_none() {
            configured_address(&self.catalog_url)
                .map(|address| {
                    let mut url = secure_url(Url::parse(address)?, "https")?;
                    strip_last_segment(&mut url);
                    Ok::<_, anyhow::Error>(url)
                })
                .transpose()?
        } else {
            None
        };
        let http = http
            .map(|address| secure_url(Url::parse(address)?, "https"))
            .transpose()?
            .or(legacy_base);
        let websocket = if let Some(address) = websocket {
            secure_url(Url::parse(address)?, "wss")?
        } else if let Some(http) = &http {
            let mut url = with_scheme(http, "wss")?;
            url.set_query(None);
            url.set_fragment(None);
            url.path_segments_mut()
                .expect("validated URL")
                .pop_if_empty()
                .push("ws");
            url
        } else {
            bail!("Адрес каталога INPX не задан");
        };
        let http = if let Some(http) = http {
            http
        } else {
            let mut url = with_scheme(&websocket, "https")?;
            strip_last_segment(&mut url);
            url
        };
        Ok((http, websocket))
    }
}

fn configured_address(address: &Option<String>) -> Option<&str> {
    address
        .as_deref()
        .map(str::trim)
        .filter(|address| !address.is_empty())
}

const LANGUAGE_ALIASES: &[&[&str]] = &[
    &["en", "eng"],
    &["ru", "rus"],
    &["uk", "ukr"],
    &["de", "deu", "ger"],
    &["fr", "fra", "fre"],
    &["es", "spa"],
];

fn primary_language(language: &str) -> Option<&str> {
    let mut parts = language.trim().split(['-', '_']);
    let code = parts.next()?;
    let valid_primary =
        (2..=3).contains(&code.len()) && code.bytes().all(|byte| byte.is_ascii_alphabetic());
    let valid_subtags = parts.all(|part| {
        (1..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
    });
    (valid_primary && valid_subtags).then_some(code)
}

fn language_aliases(code: &str) -> Option<&'static [&'static str]> {
    LANGUAGE_ALIASES
        .iter()
        .copied()
        .find(|aliases| aliases.iter().any(|alias| alias.eq_ignore_ascii_case(code)))
}

fn canonical_language(language: &str) -> Option<&str> {
    let code = primary_language(language)?;
    Some(language_aliases(code).map_or(code, |aliases| aliases[0]))
}

pub(crate) fn language_allowed(language: &str, languages: &[String]) -> bool {
    languages.is_empty()
        || language
            .split([',', ';'])
            .filter_map(canonical_language)
            .any(|code| {
                languages.iter().any(|allowed| {
                    canonical_language(allowed)
                        .is_some_and(|allowed| code.eq_ignore_ascii_case(allowed))
                })
            })
}

fn inpx_language_query(languages: &[String]) -> String {
    let mut codes = Vec::new();
    for language in languages {
        let Some(code) = primary_language(language) else {
            continue;
        };
        let fallback = [code];
        for alias in language_aliases(code).unwrap_or(&fallback) {
            let alias = alias.to_ascii_lowercase();
            if !codes.contains(&alias) {
                codes.push(alias);
            }
        }
        let tag = language.trim().to_ascii_lowercase();
        if !codes.contains(&tag) {
            codes.push(tag);
        }
    }
    codes.join(",")
}

#[derive(Clone, Debug, Default)]
pub struct Entry {
    pub title: String,
    pub author: String,
    pub language: String,
    pub series: String,
    pub description: String,
    pub metadata: String,
    pub extra_metadata: String,
    pub statistics: String,
    pub navigation: Option<String>,
    pub detail: Option<String>,
    pub cover: Option<String>,
    pub thumbnail: Option<String>,
    pub downloads: Vec<Download>,
    pub author_links: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct Download {
    pub label: String,
    pub url: String,
    pub extension: String,
}

#[derive(Clone, Debug, Default)]
pub struct Page {
    pub entries: Vec<Entry>,
    pub next: Option<NextPage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NextPage(Navigation);

impl NextPage {
    pub(crate) fn flibusta(url: String) -> Self {
        Self(Navigation::Flibusta(url))
    }

    pub(crate) fn flibusta_url(&self) -> Option<&str> {
        match &self.0 {
            Navigation::Flibusta(url) => Some(url),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Navigation {
    Books(BookQuery),
    Names {
        kind: NameKind,
        term: String,
        offset: usize,
    },
    Entity {
        kind: NameKind,
        id: Option<u64>,
        name: String,
        page: usize,
    },
    Genres,
    GenreSection(String),
    Flibusta(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct BookQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    series: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    genre: Option<String>,
    offset: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum NameKind {
    Author,
    Series,
}

impl NameKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Author => "author",
            Self::Series => "series",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SearchMode {
    Books,
    Authors,
}

pub struct Client {
    base: Url,
    websocket: Url,
    http: ureq::Agent,
    languages: Vec<String>,
    language_query: String,
    details: Mutex<DetailCache>,
    genre_names: Mutex<HashMap<String, String>>,
}

#[derive(Default)]
struct DetailCache {
    entries: VecDeque<(String, BookPreview)>,
    gates: HashMap<String, Weak<Mutex<()>>>,
}

impl DetailCache {
    fn get(&mut self, uid: &str) -> Option<BookPreview> {
        let index = self.entries.iter().position(|(key, _)| key == uid)?;
        let cached = self.entries.remove(index)?;
        let detail = cached.1.clone();
        self.entries.push_back(cached);
        Some(detail)
    }

    fn insert(&mut self, uid: String, detail: BookPreview) {
        if self.entries.len() == DETAIL_CACHE_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back((uid, detail));
    }

    fn gate(&mut self, uid: &str) -> Arc<Mutex<()>> {
        self.gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = self.gates.get(uid).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        self.gates.insert(uid.to_owned(), Arc::downgrade(&gate));
        gate
    }
}

pub(crate) fn http_agent(max_redirects: u32) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .https_only(true)
        .timeout_connect(Some(SOCKET_TIMEOUT))
        .timeout_per_read(Some(Duration::from_secs(20)))
        .max_redirects(max_redirects)
        .build();
    ureq::Agent::with_parts(
        config,
        DefaultConnector::default(),
        CatalogResolver::default(),
    )
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        let (base, websocket) = config.endpoints()?;
        let language_query = inpx_language_query(&config.languages);
        Ok(Self {
            base,
            websocket,
            http: http_agent(5),
            languages: config.languages,
            language_query,
            details: Mutex::default(),
            genre_names: Mutex::default(),
        })
    }

    pub fn root(&self) -> Result<Page> {
        let names = |kind| Navigation::Names {
            kind,
            term: String::new(),
            offset: 0,
        };
        Ok(Page {
            entries: vec![
                folder("Книги", Navigation::Books(BookQuery::default()))?,
                folder("Авторы", names(NameKind::Author))?,
                folder("Жанры", Navigation::Genres)?,
            ],
            next: None,
        })
    }

    pub fn page(&self, address: &str) -> Result<Page> {
        let target = serde_json::from_str(address).context("Invalid INPX catalog link")?;
        self.navigate(&target)
    }

    pub fn search(&self, mode: SearchMode, term: &str) -> Result<Page> {
        let term = term.trim();
        ensure!(!term.is_empty(), "Enter a search term");

        match mode {
            SearchMode::Books => {
                for query in inpx_queries(term) {
                    let page = self.books(&query)?;
                    if !page.entries.is_empty() || page.next.is_some() {
                        return Ok(page);
                    }
                }
                Ok(Page::default())
            }
            SearchMode::Authors => self.names(NameKind::Author, term, 0),
        }
    }

    pub fn books_by_author(&self, author: &str) -> Result<Page> {
        let author = author.trim();
        ensure!(!author.is_empty(), "Enter an author name");
        self.entity(NameKind::Author, None, author, 1)
    }

    pub fn more(&self, next: &NextPage) -> Result<Page> {
        self.navigate(&next.0)
    }

    fn navigate(&self, target: &Navigation) -> Result<Page> {
        match target {
            Navigation::Books(query) => self.books(query),
            Navigation::Names { kind, term, offset } => self.names(*kind, term, *offset),
            Navigation::Entity {
                kind,
                id,
                name,
                page,
            } => self.entity(*kind, *id, name, *page),
            Navigation::Genres => self.genres(None),
            Navigation::GenreSection(name) => self.genres(Some(name)),
            Navigation::Flibusta(_) => bail!("Flibusta page does not belong to INPX"),
        }
    }

    fn books(&self, query: &BookQuery) -> Result<Page> {
        let mut query = query.clone();
        let mut lookahead = FILTERED_PAGE_LOOKAHEAD;
        loop {
            let mut wire_query = serde_json::to_value(&query)?;
            wire_query["del"] = json!("0");
            wire_query["limit"] = json!(INPX_PAGE_SIZE);
            self.add_language_filter(&mut wire_query);
            let response: SearchResults<InpxBook> =
                self.request("bookSearch", json!({ "query": wire_query }))?;
            let page = self.book_page(&query, response)?;
            if !page.entries.is_empty() || lookahead == 0 {
                return Ok(page);
            }
            let Some(NextPage(Navigation::Books(next))) = &page.next else {
                return Ok(page);
            };
            query = next.clone();
            lookahead -= 1;
        }
    }

    fn book_page(&self, query: &BookQuery, response: SearchResults<InpxBook>) -> Result<Page> {
        let next =
            next_offset(query.offset, response.found.len(), response.total_found)?.map(|offset| {
                NextPage(Navigation::Books(BookQuery {
                    offset,
                    ..query.clone()
                }))
            });
        let entries = self.allowed_entries(response.found);
        Ok(Page { entries, next })
    }

    fn names(&self, kind: NameKind, term: &str, offset: usize) -> Result<Page> {
        let mut query = json!({ "del": "0", "limit": INPX_PAGE_SIZE, "offset": offset });
        if !term.is_empty() {
            query[kind.as_str()] = json!(format!("*{term}"));
        }
        self.add_language_filter(&mut query);
        let response: SearchResults<InpxName> =
            self.request("search", json!({ "from": kind.as_str(), "query": query }))?;
        let next = next_offset(offset, response.found.len(), response.total_found)?.map(|offset| {
            NextPage(Navigation::Names {
                kind,
                term: term.to_owned(),
                offset,
            })
        });
        let entries = response
            .found
            .into_iter()
            .map(|row| {
                let (id, name) = row.name(kind)?;
                folder(
                    &name,
                    Navigation::Entity {
                        kind,
                        id: Some(id),
                        name: name.clone(),
                        page: 1,
                    },
                )
            })
            .collect::<Result<_>>()?;
        Ok(Page { entries, next })
    }

    fn entity(&self, kind: NameKind, id: Option<u64>, name: &str, page: usize) -> Result<Page> {
        let mut page = page;
        let mut lookahead = FILTERED_PAGE_LOOKAHEAD;
        let mut filters = json!({});
        self.add_language_filter(&mut filters);
        loop {
            let response: EntityResults = match self.request(
                "catalog-entity",
                json!({
                    "options": {
                        "kind": kind, "name": name, "page": page, "limit": INPX_PAGE_SIZE,
                        "filters": filters
                    }
                }),
            ) {
                Err(error) if error.is::<UnknownAction>() => {
                    return self.legacy_entity(kind, id, name);
                }
                result => result?,
            };
            let next_page = (response.page >= page && response.page < response.page_count)
                .then(|| response.page + 1);
            let entries = self.allowed_entries(response.books);
            if let Some(next_page) = next_page
                && entries.is_empty()
                && lookahead > 0
            {
                page = next_page;
                lookahead -= 1;
                continue;
            }
            let next = next_page.map(|page| {
                NextPage(Navigation::Entity {
                    kind,
                    id,
                    name: name.to_owned(),
                    page,
                })
            });
            return Ok(Page { entries, next });
        }
    }

    fn legacy_entity(&self, kind: NameKind, id: Option<u64>, name: &str) -> Result<Page> {
        match kind {
            NameKind::Author => {
                // The exact bookSearch author predicate matches the whole field,
                // which would miss books listing this author among several.
                let id = match id {
                    Some(id) => id,
                    None => {
                        let mut query =
                            json!({ "author": format!("={name}"), "del": "0", "limit": 1 });
                        self.add_language_filter(&mut query);
                        let authors: SearchResults<InpxName> =
                            self.request("search", json!({ "from": "author", "query": query }))?;
                        let Some(author) = authors.found.first() else {
                            return Ok(Page::default());
                        };
                        author.id
                    }
                };
                let mut query = json!({ "del": "0" });
                self.add_language_filter(&mut query);
                let response: LegacyAuthorResults = self.request(
                    "get-author-book-list",
                    json!({ "authorId": id, "query": query }),
                )?;
                let entries = self.allowed_entries(response.books);
                Ok(Page {
                    entries,
                    next: None,
                })
            }
            NameKind::Series => self.books(&BookQuery {
                series: Some(format!("={name}")),
                ..BookQuery::default()
            }),
        }
    }

    fn add_language_filter(&self, query: &mut Value) {
        if !self.language_query.is_empty() {
            query["lang"] = json!(self.language_query);
        }
    }

    fn allowed_entries(&self, books: Vec<InpxBook>) -> Vec<Entry> {
        books
            .into_iter()
            .filter(|book| {
                book.del == 0
                    && language_allowed(book.lang.as_deref().unwrap_or_default(), &self.languages)
            })
            .map(|book| self.book_entry(book))
            .collect()
    }

    fn genres(&self, section: Option<&str>) -> Result<Page> {
        let response: GenreTree = self.request("get-genre-tree", json!({}))?;
        *self.genre_names.lock().expect("genre names lock poisoned") = response
            .genre_tree
            .iter()
            .flat_map(|section| &section.value)
            .map(|genre| (genre.value.clone(), genre.name.clone()))
            .collect();
        let entries = if let Some(section) = section {
            let section = response
                .genre_tree
                .into_iter()
                .find(|group| group.name == section)
                .context("INPX genre section no longer exists")?;
            section
                .value
                .into_iter()
                .map(|genre| {
                    folder(
                        &genre.name,
                        Navigation::Books(BookQuery {
                            genre: Some(genre.value),
                            ..BookQuery::default()
                        }),
                    )
                })
                .collect::<Result<_>>()?
        } else {
            response
                .genre_tree
                .into_iter()
                .map(|group| folder(&group.name, Navigation::GenreSection(group.name.clone())))
                .collect::<Result<_>>()?
        };
        Ok(Page {
            entries,
            next: None,
        })
    }

    fn book_entry(&self, book: InpxBook) -> Entry {
        let extension = book.ext.trim().trim_start_matches('.').to_ascii_lowercase();
        let format = extension.to_ascii_uppercase();
        let language = book.lang.unwrap_or_default().trim().to_owned();
        let series = book.series.unwrap_or_default().trim().to_owned();
        let mut metadata = String::new();
        append_metadata(&mut metadata, "Язык книги", &language);
        append_metadata(&mut metadata, "Серия", &series);
        if let Some(genres) = &book.genre {
            let names = self.genre_names.lock().expect("genre names lock poisoned");
            let mut labels = Vec::new();
            for genre in genres
                .split(',')
                .map(str::trim)
                .filter(|genre| !genre.is_empty())
            {
                let label = names.get(genre).map_or(genre, String::as_str);
                if !labels.contains(&label) {
                    labels.push(label);
                }
            }
            append_metadata(&mut metadata, "Жанры", &labels.join(", "));
        }
        let mut download = self.endpoint(&["book", "by-uid"]);
        download
            .query_pairs_mut()
            .append_pair("uid", &book.uid)
            .append_pair("format", "raw");
        let mut thumbnail = self.endpoint(&["cover", "by-uid"]);
        thumbnail.query_pairs_mut().append_pair("uid", &book.uid);
        let downloads = if valid_extension(&extension) {
            vec![Download {
                label: format,
                url: download.into(),
                extension: extension.clone(),
            }]
        } else {
            Vec::new()
        };
        Entry {
            title: book.title,
            author: book.author,
            language,
            series,
            metadata,
            detail: Some(book.uid),
            thumbnail: Some(thumbnail.into()),
            downloads,
            ..Entry::default()
        }
    }

    pub fn detail(&self, entry: &Entry) -> Result<Entry> {
        let Some(uid) = &entry.detail else {
            return Ok(entry.clone());
        };
        let preview = self.load_detail(uid, || {
            let card = self.request::<BookInfoResponse>(
                "get-book-info",
                json!({ "bookUid": uid, "card": true, "lazyExtras": true }),
            );
            let card_supported = match card {
                Ok(response) => {
                    if let Some(details) = response.details() {
                        return Ok(details);
                    }
                    true
                }
                Err(error) if error.is::<UnknownAction>() => false,
                Err(error) => return Err(error),
            };
            // The lightweight endpoint also supports formats without an FB2 card.
            let preview =
                self.request::<BookPreviewResponse>("get-book-preview", json!({ "bookUid": uid }));
            match preview {
                Ok(response) => Ok(response.book_preview),
                Err(error) if card_supported && error.is::<UnknownAction>() => {
                    Ok(BookPreview::default())
                }
                Err(error) => Err(error),
            }
        })?;
        ensure!(
            preview.annotation_status != "unavailable",
            "INPX could not load the annotation"
        );
        Ok(preview.apply(entry))
    }

    fn load_detail(
        &self,
        uid: &str,
        load: impl FnOnce() -> Result<BookPreview>,
    ) -> Result<BookPreview> {
        let gate = {
            let mut cache = self.details.lock().expect("detail cache lock poisoned");
            if let Some(detail) = cache.get(uid) {
                return Ok(detail);
            }
            cache.gate(uid)
        };

        // A waiter for the same book checks the cache again when its request completes.
        let _request = gate.lock().expect("detail request lock poisoned");
        if let Some(detail) = self
            .details
            .lock()
            .expect("detail cache lock poisoned")
            .get(uid)
        {
            return Ok(detail);
        }
        let detail = load()?;
        if detail.annotation_status != "unavailable" {
            self.details
                .lock()
                .expect("detail cache lock poisoned")
                .insert(uid.to_owned(), detail.clone());
        }
        Ok(detail)
    }

    pub fn cover(&self, address: &str) -> Result<Vec<u8>> {
        let url = secure_url(self.base.join(address)?, "https")?;
        let mut response = self
            .http
            .get(url.as_str())
            .call()
            .context("Loading book cover")?;
        Ok(response
            .body_mut()
            .with_config()
            .limit(COVER_LIMIT as u64)
            .read_to_vec()?)
    }

    pub fn download(
        &self,
        download: &Download,
        entry: &Entry,
        directory: &Path,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<PathBuf> {
        let url = secure_url(self.base.join(&download.url)?, "https")?;
        ensure!(
            valid_extension(&download.extension),
            "Invalid book file extension"
        );
        let response = self
            .http
            .get(url.as_str())
            .call()
            .context("Downloading book")?;
        save_book_response(response, download, entry, directory, progress)
    }

    fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .expect("validated HTTPS URL")
            .pop_if_empty()
            .extend(segments.iter().copied());
        url
    }

    fn request<T: DeserializeOwned>(&self, action: &str, mut request: Value) -> Result<T> {
        let host = self.websocket.host_str().expect("validated WebSocket host");
        let port = self.websocket.port().unwrap_or(443);
        let mut connection = None;
        let mut last_error = None;
        let addresses = (host, port)
            .to_socket_addrs()
            .context(NetworkFailure("Resolving INPX host"))?;
        for address in addresses {
            match TcpStream::connect_timeout(&address, SOCKET_TIMEOUT) {
                Ok(stream) => {
                    connection = Some(stream);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let stream = match (connection, last_error) {
            (Some(stream), _) => stream,
            (None, Some(error)) => return Err(error).context(NetworkFailure("Connecting to INPX")),
            (None, None) => return Err(NetworkFailure("No INPX address found").into()),
        };
        let read_timeout = if matches!(action, "get-book-preview" | "get-book-info") {
            Duration::from_secs(15)
        } else {
            SOCKET_TIMEOUT
        };
        stream.set_read_timeout(Some(read_timeout))?;
        stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
        let config = tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(MESSAGE_LIMIT))
            .max_frame_size(Some(MESSAGE_LIMIT));
        let mut handshake = self.websocket.as_str().into_client_request()?;
        let origin = with_scheme(&self.websocket, "https")?;
        handshake
            .headers_mut()
            .insert(ORIGIN, origin.origin().ascii_serialization().parse()?);
        let (mut socket, _) = client_tls_with_config(handshake, stream, Some(config), None)
            .map_err(|error| match error {
                HandshakeError::Failure(error) => {
                    anyhow::Error::new(error).context("Connecting to INPX")
                }
                HandshakeError::Interrupted(_) => {
                    NetworkFailure("INPX handshake interrupted").into()
                }
            })?;
        request["action"] = json!(action);
        socket.send(Message::Text(request.to_string().into()))?;
        for _ in 0..8 {
            let message = socket.read().context("Reading INPX response")?;
            if let Message::Text(text) = message {
                if let Some(result) = decode_response(text.as_str(), action)? {
                    let _ = socket.close(None);
                    return Ok(result);
                }
            }
        }
        bail!("INPX {action} returned no result message")
    }
}

pub(crate) fn save_book_response(
    mut response: ureq::http::Response<ureq::Body>,
    download: &Download,
    entry: &Entry,
    directory: &Path,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<PathBuf> {
    let extension = &download.extension;
    ensure!(valid_extension(extension), "Invalid book file extension");
    let headers = response.headers();
    ensure!(
        !headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| {
                let value = value.trim();
                value.eq_ignore_ascii_case("text/html")
                    || value.eq_ignore_ascii_case("application/xhtml+xml")
            }),
        "The catalog returned a web page instead of a book"
    );
    let total = headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    fs::create_dir_all(directory).with_context(|| format!("Creating {}", directory.display()))?;
    write_book(
        &mut response.body_mut().as_reader(),
        directory,
        &safe_filename(&entry.title),
        extension,
        total,
        progress,
    )
}

fn write_book(
    input: &mut impl Read,
    directory: &Path,
    stem: &str,
    extension: &str,
    total: Option<u64>,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<PathBuf> {
    let mut partial = Builder::new()
        .prefix(".book-")
        .suffix(".part")
        .tempfile_in(directory)?;
    let mut received = 0;
    let mut buffer = [0; 16 * 1024];
    progress(received, total);
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(ureq::Error::from)
            .context("Reading book download")?;
        if count == 0 {
            break;
        }
        partial
            .write_all(&buffer[..count])
            .context("Writing book download")?;
        received += count as u64;
        progress(received, total);
    }
    if let Some(total) = total {
        if received < total {
            return Err(NetworkFailure("The book download was incomplete").into());
        }
        ensure!(received == total, "The book download was incomplete");
    }
    ensure!(received > 0, "The catalog returned an empty book file");
    partial
        .as_file()
        .sync_all()
        .context("Saving book download")?;

    let mut use_reservation = false;
    for attempt in 1..=999 {
        let name = if attempt == 1 {
            format!("{stem}.{extension}")
        } else {
            format!("{stem} ({attempt}).{extension}")
        };
        let path = directory.join(name);
        if !use_reservation {
            match partial.persist_noclobber(&path) {
                Ok(_) => return Ok(path),
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    partial = error.file;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.error.kind(),
                        io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    partial = error.file;
                    use_reservation = true;
                }
                Err(error) => {
                    return Err(error.error).with_context(|| format!("Saving {}", path.display()));
                }
            }
        }

        // Older Linux kernels fall back to hard links, which FAT does not support.
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(reservation) => drop(reservation),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("Saving {}", path.display()));
            }
        }
        if let Err(error) = partial.persist(&path) {
            let _ = fs::remove_file(&path);
            return Err(error.error).with_context(|| format!("Saving {}", path.display()));
        }
        return Ok(path);
    }
    bail!("Too many files named {stem}")
}

fn inpx_queries(term: &str) -> Vec<BookQuery> {
    let term_query = format!("*{term}");
    let mut queries = vec![BookQuery {
        title: Some(term_query.clone()),
        ..BookQuery::default()
    }];

    // Try a surname at either end when the complete term didn't match a title.
    let words: Vec<_> = term.split_whitespace().collect();
    if words.len() >= 2 {
        let last = words.len() - 1;
        for split in std::iter::once(1).chain((last > 1).then_some(last)) {
            let left = words[..split].join(" ");
            let right = words[split..].join(" ");
            for (title, author) in [(&right, &left), (&left, &right)] {
                let query = BookQuery {
                    title: Some(format!("*{title}")),
                    author: Some(format!("*{author}")),
                    ..BookQuery::default()
                };
                if !queries.contains(&query) {
                    queries.push(query);
                }
            }
        }
    }

    queries.push(BookQuery {
        author: Some(term_query),
        ..BookQuery::default()
    });
    queries
}

pub(crate) fn secure_url(url: Url, scheme: &str) -> Result<Url> {
    ensure!(
        url.scheme() == scheme
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none(),
        "Expected a {scheme} URL without embedded credentials"
    );
    Ok(url)
}

fn safe_filename(title: &str) -> String {
    let mut stem = String::new();
    for character in title.trim().chars() {
        if stem.len() + character.len_utf8() > 150 {
            break;
        }
        if character.is_control()
            || matches!(
                character,
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
            )
        {
            stem.push('_');
        } else {
            stem.push(character);
        }
    }
    let stem = stem.trim_matches(|character: char| character == '.' || character.is_whitespace());
    let first = stem
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let reserved = matches!(first.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (first.len() == 4
            && (first.starts_with("COM") || first.starts_with("LPT"))
            && matches!(first.as_bytes()[3], b'1'..=b'9'));
    if stem.is_empty() {
        "book".into()
    } else if reserved {
        format!("_{stem}")
    } else {
        stem.into()
    }
}

#[cfg(test)]
#[test]
fn incomplete_book_does_not_replace_existing_download() {
    let dir = tempfile::tempdir().unwrap();
    let existing = dir.path().join("Книга.fb2");
    fs::write(&existing, b"old").unwrap();

    let mut broken = (&b"new"[..]).chain(BrokenStream);
    let result = write_book(
        &mut broken,
        dir.path(),
        "Книга",
        "fb2",
        Some(9),
        &mut |_, _| {},
    );
    assert!(is_network_error(&result.unwrap_err()));
    let incomplete = write_book(
        &mut &b"new"[..],
        dir.path(),
        "Книга",
        "fb2",
        Some(9),
        &mut |_, _| {},
    )
    .unwrap_err();
    assert!(is_network_error(&incomplete));
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    assert_eq!(fs::read(&existing).unwrap(), b"old");

    let saved = write_book(
        &mut &b"new"[..],
        dir.path(),
        "Книга",
        "fb2",
        Some(3),
        &mut |_, _| {},
    )
    .unwrap();
    assert_ne!(saved, existing);
    assert_eq!(fs::read(saved).unwrap(), b"new");
    assert_eq!(fs::read(existing).unwrap(), b"old");

    struct BrokenStream;
    impl Read for BrokenStream {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "test connection dropped",
            ))
        }
    }
}

#[test]
fn reconnect_only_for_transport_errors() {
    let dns = Err::<(), _>(io::Error::other("resolver failed"))
        .context(NetworkFailure("Resolving INPX host"))
        .unwrap_err();
    let websocket = anyhow::Error::new(tungstenite::Error::Protocol(
        ProtocolError::ResetWithoutClosingHandshake,
    ))
    .context("Reading INPX response");
    let http_dns = anyhow::Error::new(ureq::Error::HostNotFound).context("Downloading book");
    let body_timeout = anyhow::Error::new(ureq::Error::from(
        ureq::Error::Timeout(ureq::Timeout::PerRead).into_io(),
    ))
    .context("Reading book download");
    for error in [dns, websocket, http_dns, body_timeout] {
        assert!(is_network_error(&error), "{error:#}");
    }

    let http_status = anyhow::Error::new(ureq::Error::StatusCode(404)).context("Downloading book");
    let invalid_tls = anyhow::Error::new(tungstenite::Error::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid peer certificate",
    )))
    .context("Connecting to INPX");
    let disk = anyhow::Error::new(io::Error::other("disk full")).context("Writing book download");
    for error in [
        http_status,
        invalid_tls,
        disk,
        anyhow!("INPX denied request"),
    ] {
        assert!(!is_network_error(&error), "{error:#}");
    }
}

fn decode_response<T: DeserializeOwned>(text: &str, action: &str) -> Result<Option<T>> {
    let value: Value = serde_json::from_str(text).context("Decoding INPX response")?;
    if let Some(error) = value["error"].as_str() {
        if error.strip_prefix("Action not found: ") == Some(action) {
            return Err(UnknownAction(action.to_owned()).into());
        }
        bail!("INPX {action}: {error}");
    }
    if value["_rok"] == 1 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(value).with_context(|| {
        format!("Decoding INPX {action} result")
    })?))
}

fn folder(title: &str, target: Navigation) -> Result<Entry> {
    Ok(Entry {
        title: title.to_owned(),
        navigation: Some(serde_json::to_string(&target)?),
        ..Entry::default()
    })
}

fn next_offset(offset: usize, count: usize, total: usize) -> Result<Option<usize>> {
    let next = offset.saturating_add(count);
    ensure!(
        count > 0 || next >= total,
        "INPX returned an empty page before the end of the results"
    );
    Ok((next < total).then_some(next))
}

pub(crate) fn valid_extension(extension: &str) -> bool {
    !extension.is_empty()
        && extension.len() <= 12
        && extension
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_alphanumeric()))
}

fn with_scheme(url: &Url, scheme: &str) -> Result<Url> {
    let mut url = url.clone();
    url.set_scheme(scheme)
        .map_err(|_| anyhow!("Invalid INPX URL scheme"))?;
    Ok(url)
}

fn strip_last_segment(url: &mut Url) {
    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .expect("validated URL")
        .pop_if_empty()
        .pop();
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchResults<T> {
    found: Vec<T>,
    total_found: usize,
}

#[derive(Deserialize)]
struct InpxBook {
    #[serde(rename = "_uid")]
    uid: String,
    title: String,
    #[serde(default)]
    author: String,
    lang: Option<String>,
    series: Option<String>,
    genre: Option<String>,
    ext: String,
    #[serde(default)]
    del: u64,
}

#[derive(Deserialize)]
struct InpxName {
    id: u64,
    author: Option<String>,
    series: Option<String>,
}

impl InpxName {
    fn name(self, kind: NameKind) -> Result<(u64, String)> {
        let name = match kind {
            NameKind::Author => self.author,
            NameKind::Series => self.series,
        }
        .context("INPX search result has no name")?;
        Ok((self.id, name))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntityResults {
    page: usize,
    page_count: usize,
    books: Vec<InpxBook>,
}

#[derive(Deserialize)]
struct LegacyAuthorResults {
    books: Vec<InpxBook>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenreTree {
    genre_tree: Vec<GenreSection>,
}

#[derive(Deserialize)]
struct GenreSection {
    name: String,
    value: Vec<Genre>,
}

#[derive(Deserialize)]
struct Genre {
    name: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookPreviewResponse {
    book_preview: BookPreview,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookPreview {
    annotation: String,
    annotation_status: String,
    published_year: Option<String>,
    keywords: Option<String>,
    #[serde(skip)]
    full_author: String,
    #[serde(skip)]
    original_title: String,
    #[serde(skip)]
    original_language: String,
    #[serde(skip)]
    translators: String,
    #[serde(skip)]
    edition_title: String,
    #[serde(skip)]
    publisher: String,
    #[serde(skip)]
    publication_city: String,
    #[serde(skip)]
    isbn: String,
    #[serde(skip)]
    statistics: String,
}

impl BookPreview {
    fn apply(self, entry: &Entry) -> Entry {
        let mut detail = entry.clone();
        if !self.annotation.is_empty() {
            detail.description = self.annotation;
        }
        let name_separator = |character: char| character.is_whitespace() || character == ',';
        if self
            .full_author
            .split(name_separator)
            .filter(|part| !part.is_empty())
            .ne(entry
                .author
                .split(name_separator)
                .filter(|part| !part.is_empty()))
        {
            append_metadata(&mut detail.extra_metadata, "Автор(ы)", &self.full_author);
        }
        append_metadata(
            &mut detail.extra_metadata,
            "Название оригинала",
            &self.original_title,
        );
        append_metadata(
            &mut detail.extra_metadata,
            "Язык оригинала",
            &self.original_language,
        );
        append_metadata(&mut detail.extra_metadata, "Переводчики", &self.translators);
        if let Some(keywords) = self.keywords {
            append_metadata(&mut detail.extra_metadata, "Ключевые слова", &keywords);
        }
        if self
            .edition_title
            .split_whitespace()
            .ne(entry.title.split_whitespace())
        {
            append_metadata(
                &mut detail.extra_metadata,
                "Название издания",
                &self.edition_title,
            );
        }
        append_metadata(&mut detail.extra_metadata, "Издательство", &self.publisher);
        append_metadata(
            &mut detail.extra_metadata,
            "Город издания",
            &self.publication_city,
        );
        if let Some(year) = self.published_year {
            append_metadata(&mut detail.extra_metadata, "Год издания", &year);
        }
        append_metadata(&mut detail.extra_metadata, "ISBN", &self.isbn);
        detail.statistics = self.statistics;
        detail
    }
}

fn append_metadata(output: &mut String, label: &str, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(label);
    output.push_str(": ");
    output.push_str(value);
}

#[derive(Debug)]
struct UnknownAction(String);

impl std::fmt::Display for UnknownAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INPX does not support {}", self.0)
    }
}

impl std::error::Error for UnknownAction {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookInfoResponse {
    book_info: BookInfo,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookInfo {
    #[serde(default)]
    fb2: Value,
    annotation_meta: Option<AnnotationMeta>,
}

#[derive(Deserialize)]
struct AnnotationMeta {
    stats: Option<TextStatistics>,
}

#[derive(Deserialize)]
struct TextStatistics {
    letters: Option<u64>,
    words: Option<u64>,
    pages: Option<f64>,
    images: Option<u64>,
}

impl TextStatistics {
    fn display(self) -> String {
        let mut statistics = String::new();
        for (label, count) in [("Букв", self.letters), ("Слов", self.words)] {
            if let Some(count) = count {
                append_metadata(&mut statistics, label, &format_count(count));
            }
        }
        if let Some(pages) = self.pages {
            append_metadata(
                &mut statistics,
                "Страниц",
                &pages.to_string().replace('.', ","),
            );
        }
        if let Some(images) = self.images {
            append_metadata(&mut statistics, "Изображений", &format_count(images));
        }
        statistics
    }
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(' ');
        }
        formatted.push(digit);
    }
    formatted
}

impl BookInfoResponse {
    fn details(self) -> Option<BookPreview> {
        let fb2 = &self.book_info.fb2;
        let description = fb2_children(fb2.as_array()?, &["fictionbook", "description"])?;
        let title_info = fb2_children(description, &["title-info"]).unwrap_or_default();
        let source_info = fb2_children(description, &["src-title-info"]).unwrap_or_default();
        let publish_info = fb2_children(description, &["publish-info"]).unwrap_or_default();
        let publication_year = fb2_text(publish_info, &["year"]);
        let date_text = fb2_text(title_info, &["date"]);
        let date_value = fb2_element(title_info, "date")
            .and_then(|date| date[2].as_array())
            .and_then(|attrs| attrs.iter().find(|attr| attr[0] == "value"))
            .and_then(|attr| attr[1].as_str())
            .unwrap_or_default();
        let published_year = [publication_year.as_str(), date_value, date_text.as_str()]
            .into_iter()
            .find_map(year_prefix)
            .map(str::to_owned);
        let mut original_language = fb2_text(title_info, &["src-lang"]);
        if original_language.is_empty() {
            original_language = fb2_text(source_info, &["lang"]);
        }
        let statistics = self
            .book_info
            .annotation_meta
            .and_then(|meta| meta.stats)
            .map(TextStatistics::display)
            .unwrap_or_default();
        Some(BookPreview {
            annotation: fb2_text(title_info, &["annotation"])
                .chars()
                .take(6000)
                .collect(),
            annotation_status: "ready".into(),
            published_year,
            keywords: Some(fb2_text(title_info, &["keywords"])),
            full_author: fb2_people(&[title_info], "author"),
            original_title: fb2_text(source_info, &["book-title"]),
            original_language,
            translators: fb2_people(&[title_info, source_info], "translator"),
            edition_title: fb2_text(publish_info, &["book-name"]),
            publisher: fb2_text(publish_info, &["publisher"]),
            publication_city: fb2_text(publish_info, &["city"]),
            isbn: fb2_text(publish_info, &["isbn"]),
            statistics,
        })
    }
}

// inpx-web returns the legacy FB2 card as JSON node tuples: [1, tag,
// attributes, children] for elements, [2, text] and [3, text] for text and CDATA.
fn fb2_element<'a>(children: &'a [Value], tag: &str) -> Option<&'a Value> {
    children
        .iter()
        .find(|node| node[0] == 1 && node[1].as_str() == Some(tag))
}

fn fb2_children<'a>(mut children: &'a [Value], path: &[&str]) -> Option<&'a [Value]> {
    for tag in path {
        children = fb2_element(children, tag)?[3].as_array()?;
    }
    Some(children)
}

fn fb2_text(children: &[Value], path: &[&str]) -> String {
    let mut text = String::new();
    if let Some(nodes) = fb2_children(children, path) {
        append_fb2_text(nodes, &mut text);
    }
    text.trim().to_owned()
}

fn year_prefix(value: &str) -> Option<&str> {
    let year = value
        .trim()
        .split(|character: char| character.is_whitespace() || matches!(character, '-' | '/' | '.'))
        .next()?;
    (year.len() == 4 && !year.starts_with('0') && year.bytes().all(|digit| digit.is_ascii_digit()))
        .then_some(year)
}

fn fb2_people(sections: &[&[Value]], tag: &str) -> String {
    let mut people = Vec::new();
    for person in sections
        .iter()
        .flat_map(|section| section.iter())
        .filter(|node| node[0] == 1 && node[1].as_str() == Some(tag))
    {
        let Some(fields) = person[3].as_array() else {
            continue;
        };
        let parts =
            ["last-name", "first-name", "middle-name"].map(|part| fb2_text(fields, &[part]));
        let mut name = parts
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if name.is_empty() {
            name = fb2_text(fields, &["nickname"]);
        }
        if !name.is_empty() && !people.contains(&name) {
            people.push(name);
        }
    }
    people.join(", ")
}

fn append_fb2_text(nodes: &[Value], text: &mut String) {
    for node in nodes {
        match node[0].as_u64() {
            Some(1) => {
                let paragraph = matches!(
                    node[1].as_str(),
                    Some("p" | "subtitle" | "empty-line" | "stanza")
                );
                if paragraph && !text.is_empty() && !text.ends_with("\n\n") {
                    text.push_str("\n\n");
                }
                if let Some(children) = node[3].as_array() {
                    append_fb2_text(children, text);
                }
                if paragraph && !text.ends_with("\n\n") {
                    text.push_str("\n\n");
                }
            }
            Some(2 | 3) => {
                if let Some(value) = node[1].as_str() {
                    if node[0] == 2 {
                        text.push_str(&decode_entities(value));
                    } else {
                        text.push_str(value);
                    }
                }
            }
            _ => {}
        }
    }
}

fn decode_entity(name: &str) -> Option<char> {
    let character = match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "laquo" => '«',
        "raquo" => '»',
        "nbsp" => ' ',
        "mdash" => '—',
        "ndash" => '–',
        "hellip" => '…',
        _ => {
            let number = name.strip_prefix('#')?;
            let code = if let Some(hex) = number.strip_prefix('x') {
                u32::from_str_radix(hex, 16).ok()?
            } else {
                number.parse().ok()?
            };
            char::from_u32(code)?
        }
    };
    Some(character)
}

pub(crate) fn decode_entities(value: &str) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while let Some((before, after)) = remaining.split_once('&') {
        output.push_str(before);
        let reference = after.split_once(';').filter(|(name, _)| name.len() <= 12);
        if let Some((name, rest)) = reference {
            if let Some(character) = decode_entity(name) {
                output.push(character);
                remaining = rest;
                continue;
            }
        }
        output.push('&');
        remaining = after;
    }
    output.push_str(remaining);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_filter_keeps_the_server_cursor_and_supports_legacy_configs() {
        let config: Config =
            toml::from_str(r#"inpx_ws_url = "wss://books.example/library/ws""#).unwrap();
        assert_eq!(config.languages, ["en", "ru"]);
        assert_eq!(config.flibusta.opds_url, "https://flibusta.is/opds");
        assert!(language_allowed(" rus ; de ", &config.languages));
        assert!(language_allowed("en-GB", &config.languages));
        assert!(!language_allowed("de", &config.languages));
        assert!(!language_allowed("", &config.languages));

        let client = Client::new(config).unwrap();
        let mut wire_query = json!({});
        client.add_language_filter(&mut wire_query);
        assert_eq!(wire_query["lang"], "en,eng,ru,rus");
        let response = serde_json::from_value(json!({
            "found": [
                {"_uid":"en","title":"English","ext":"epub","lang":"en"},
                {"_uid":"de","title":"German","ext":"fb2","lang":"de"},
                {"_uid":"unknown","title":"Unknown","ext":"fb2"}
            ],
            "totalFound": 5
        }))
        .unwrap();
        let page = client.book_page(&BookQuery::default(), response).unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].language, "en");
        assert_eq!(
            page.next,
            Some(NextPage(Navigation::Books(BookQuery {
                offset: 3,
                ..BookQuery::default()
            })))
        );

        let config = Config {
            inpx_ws_url: Some("wss://books.example/library/ws".into()),
            languages: Vec::new(),
            ..Config::default()
        };
        assert!(language_allowed("", &config.languages));
        let client = Client::new(config).unwrap();
        let mut wire_query = json!({});
        client.add_language_filter(&mut wire_query);
        assert_eq!(wire_query, json!({}));
    }

    #[test]
    fn search_results_offer_original_files_before_detail_and_keep_the_query() {
        let config = toml::from_str(r#"inpx_ws_url = "wss://books.example/library/ws""#).unwrap();
        let client = Client::new(config).unwrap();
        let query = BookQuery {
            title: Some("*Мастер".into()),
            author: Some("*Булгаков".into()),
            offset: 40,
            ..BookQuery::default()
        };
        assert!(inpx_queries("Булгаков Мастер").contains(&BookQuery {
            offset: 0,
            ..query.clone()
        }));

        let response = decode_response(
            r#"{"found":[
                {"_uid":"Ab+C/=","title":"Мастер","author":"Булгаков","ext":"fb2","lang":"ru","series":"Классика","genre":"prose_classic,unknown_genre"},
                {"_uid":"Epub","title":"Роман","author":"Автор","ext":"epub","lang":"ru"}
            ],"totalFound":45}"#,
            "bookSearch",
        )
        .unwrap()
        .unwrap();
        client
            .genre_names
            .lock()
            .unwrap()
            .insert("prose_classic".into(), "Классическая проза".into());
        let page = client.book_page(&query, response).unwrap();

        let fb2 = &page.entries[0];
        assert_eq!(fb2.series, "Классика");
        assert_eq!(
            fb2.metadata,
            "Язык книги: ru\nСерия: Классика\nЖанры: Классическая проза, unknown_genre"
        );
        assert_eq!(fb2.detail.as_deref(), Some("Ab+C/="));
        assert_eq!(
            fb2.thumbnail.as_deref(),
            Some("https://books.example/library/cover/by-uid?uid=Ab%2BC%2F%3D")
        );
        assert_eq!(fb2.downloads.len(), 1);
        assert_eq!(fb2.downloads[0].label, "FB2");
        assert_eq!(fb2.downloads[0].extension, "fb2");
        assert_eq!(
            fb2.downloads[0].url,
            "https://books.example/library/book/by-uid?uid=Ab%2BC%2F%3D&format=raw"
        );
        assert_eq!(page.entries[1].downloads[0].extension, "epub");
        assert!(
            page.entries[1].downloads[0]
                .url
                .ends_with("?uid=Epub&format=raw")
        );
        assert_eq!(
            page.next,
            Some(NextPage(Navigation::Books(BookQuery {
                offset: 42,
                ..query
            })))
        );
    }

    #[test]
    fn preview_preserves_the_existing_original_download() {
        let config = toml::from_str(r#"inpx_ws_url = "wss://books.example/library/ws""#).unwrap();
        let client = Client::new(config).unwrap();
        let book =
            serde_json::from_str(r#"{"_uid":"uid","title":"Book","ext":"epub","lang":"en"}"#)
                .unwrap();
        let entry = client.book_entry(book);
        let response: BookPreviewResponse = decode_response(
            r#"{"bookPreview":{"bookUid":"uid","annotation":"One paragraph.\n\nAnother.","annotationStatus":"ready","publishedYear":"2026"}}"#,
            "get-book-preview",
        ).unwrap().unwrap();
        let detail = response.book_preview.apply(&entry);
        assert_eq!(detail.description, "One paragraph.\n\nAnother.");
        assert_eq!(detail.metadata, "Язык книги: en");
        assert_eq!(detail.extra_metadata, "Год издания: 2026");
        assert_eq!(detail.language, "en");
        assert_eq!(detail.downloads[0].url, entry.downloads[0].url);
    }

    #[test]
    fn fb2_card_retains_annotation_paragraphs_and_display_metadata() {
        let response = json!({"bookInfo": {"fb2": [
            [1, "fictionbook", null, [
                [1, "description", null, [
                    [1, "title-info", null, [
                        [1, "annotation", null, [
                            [1, "p", null, [[2, "Begin &amp; "], [1, "emphasis", null, [[2, "end."]]]]],
                            [4, "comment"],
                            [1, "p", null, [[3, "Next &amp; paragraph."]]]
                        ]],
                        [1, "keywords", null, [[2, "test"]]],
                        [1, "date", [["value", "2025-03-24"]], [[2, "2024"]]],
                        [1, "author", null, [
                            [1, "last-name", null, [[2, "Лавкрафт"]]],
                            [1, "first-name", null, [[2, "Говард"]]],
                            [1, "middle-name", null, [[2, "Филлипс"]]]
                        ]],
                        [1, "translator", null, [
                            [1, "last-name", null, [[2, "Иванова"]]],
                            [1, "first-name", null, [[2, "Анна"]]]
                        ]],
                        [1, "translator", null, [
                            [1, "nickname", null, [[2, "Переводчик 2"]]]
                        ]]
                    ]],
                    [1, "src-title-info", null, [
                        [1, "book-title", null, [[2, "The Call of Cthulhu"]]],
                        [1, "lang", null, [[2, "en"]]],
                        [1, "translator", null, [
                            [1, "nickname", null, [[2, "Переводчик 2"]]]
                        ]],
                        [1, "translator", null, [
                            [1, "nickname", null, [[2, "Переводчик 3"]]]
                        ]]
                    ]],
                    [1, "publish-info", null, [
                        [1, "book-name", null, [[2, "Сборник рассказов"]]],
                        [1, "publisher", null, [[2, "Азбука"]]],
                        [1, "city", null, [[2, "Санкт-Петербург"]]],
                        [1, "year", null, [[2, "2026"]]],
                        [1, "isbn", null, [[2, "978-5-389-00000-0"]]]
                    ]]
                ]],
                [1, "body", null, [[2, "body text"]]]
            ]]
        ], "annotationMeta":{"stats":{"letters":438426,"words":74416,"pages":297.7,"images":0}}}});
        let result: BookInfoResponse = serde_json::from_value(response).unwrap();
        let entry = Entry {
            title: "Зов Ктулху".into(),
            author: "Лавкрафт Говард".into(),
            ..Entry::default()
        };
        let detail = result.details().unwrap().apply(&entry);
        assert_eq!(detail.description, "Begin & end.\n\nNext &amp; paragraph.");
        assert_eq!(
            detail.extra_metadata,
            concat!(
                "Автор(ы): Лавкрафт Говард Филлипс\n",
                "Название оригинала: The Call of Cthulhu\n",
                "Язык оригинала: en\n",
                "Переводчики: Иванова Анна, Переводчик 2, Переводчик 3\n",
                "Ключевые слова: test\n",
                "Название издания: Сборник рассказов\n",
                "Издательство: Азбука\n",
                "Город издания: Санкт-Петербург\n",
                "Год издания: 2026\n",
                "ISBN: 978-5-389-00000-0",
            )
        );
        assert_eq!(
            detail.statistics,
            "Букв: 438 426\nСлов: 74 416\nСтраниц: 297,7\nИзображений: 0"
        );
    }

    #[test]
    fn native_acknowledgments_and_unknown_actions_are_distinguished() {
        let ack = decode_response::<BookPreview>(r#"{"_rok":1}"#, "get-book-preview").unwrap();
        assert!(ack.is_none());
        let unavailable = decode_response::<BookPreview>(
            r#"{"error":"Action not found: get-book-preview"}"#,
            "get-book-preview",
        )
        .err()
        .unwrap();
        assert!(unavailable.is::<UnknownAction>());
        let unavailable =
            decode_response::<BookPreview>(r#"{"error":"404 Файл не найден"}"#, "get-book-preview")
                .err()
                .unwrap();
        assert!(!unavailable.is::<UnknownAction>());
    }

    #[test]
    fn inpx_requires_a_configured_address() {
        for config in [
            Config::default(),
            toml::from_str(
                r#"
                inpx_ws_url = ""
                inpx_http_url = " "
                catalog_url = ""
                "#,
            )
            .unwrap(),
        ] {
            assert!(!config.inpx_enabled());
            assert!(Client::new(config).is_err());
        }

        let config: Config = toml::from_str(
            r#"
            inpx_ws_url = ""
            inpx_http_url = "https://books.example/library/"
            "#,
        )
        .unwrap();
        assert!(config.inpx_enabled());
        let (http, ws) = config.endpoints().unwrap();
        assert_eq!(http.as_str(), "https://books.example/library/");
        assert_eq!(ws.as_str(), "wss://books.example/library/ws");
    }

    #[test]
    fn legacy_config_preserves_its_server_and_ignores_superseded_values() {
        let config: Config =
            toml::from_str(r#"catalog_url = "https://books.example/library/opds""#).unwrap();
        assert!(config.inpx_enabled());
        let (http, ws) = config.endpoints().unwrap();
        assert_eq!(http.as_str(), "https://books.example/library");
        assert_eq!(ws.as_str(), "wss://books.example/library/ws");

        let config: Config = toml::from_str(
            r#"
            catalog_url = "unused"
            inpx_ws_url = "wss://books.example/ws"
            inpx_http_url = "https://assets.example/library/"
        "#,
        )
        .unwrap();
        assert!(config.inpx_enabled());
        let (http, ws) = config.endpoints().unwrap();
        assert_eq!(http.as_str(), "https://assets.example/library/");
        assert_eq!(ws.as_str(), "wss://books.example/ws");
    }

    #[test]
    fn filenames_fit_on_the_reader_and_cannot_escape_the_directory() {
        assert_eq!(safe_filename("../Роман: часть 1?"), "_Роман_ часть 1_");
        assert_eq!(safe_filename("NUL.txt"), "_NUL.txt");
        assert_eq!(safe_filename(" ... "), "book");
        assert!(safe_filename(&"К".repeat(100)).len() <= 150);
    }
}
