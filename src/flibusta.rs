use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use roxmltree::{Document, Node};
use ureq::http::{
    HeaderValue, Response,
    header::{ACCEPT, AUTHORIZATION, LOCATION},
};
use url::Url;

use crate::catalog::{
    COVER_LIMIT, Download, Entry, FlibustaConfig, NextPage, Page, SearchMode, decode_entities,
    http_agent, language_allowed, save_book_response, secure_url,
};

const XML_LIMIT: u64 = 4 * 1024 * 1024;
const MAX_REDIRECTS: usize = 5;
const MAX_FILTERED_PAGES: usize = 4;
const ATOM: &str = "application/atom+xml";
const OPENSEARCH: &str = "application/opensearchdescription+xml";

pub struct FlibustaClient {
    opds: Url,
    authorization: Option<HeaderValue>,
    http: ureq::Agent,
    languages: Vec<String>,
    search: Mutex<Option<SearchTemplate>>,
}

#[derive(Clone)]
struct SearchTemplate {
    base: Url,
    template: String,
}

struct Feed {
    page: Page,
    direct_search: Option<String>,
    search_descriptor: Option<Url>,
    shelf: Option<Url>,
}

impl FlibustaClient {
    pub fn new(config: FlibustaConfig, languages: Vec<String>) -> Result<Self> {
        let opds = secure_url(Url::parse(&config.opds_url)?, "https")?;
        let authorization = match (config.username, config.password) {
            (None, None) => None,
            (Some(username), Some(password)) => {
                ensure!(
                    !username.is_empty() && !username.contains(':'),
                    "Некорректный логин Флибусты"
                );
                let encoded = STANDARD.encode(format!("{username}:{password}"));
                let mut header = HeaderValue::from_str(&format!("Basic {encoded}"))
                    .map_err(|_| anyhow!("Не удалось подготовить данные для входа во Флибусту"))?;
                header.set_sensitive(true);
                Some(header)
            }
            _ => bail!("Укажи и логин, и пароль Флибусты"),
        };
        Ok(Self {
            opds,
            authorization,
            http: http_agent(0),
            languages,
            search: Mutex::new(None),
        })
    }

    pub fn root(&self) -> Result<Page> {
        let (feed, base) = self.read_feed(self.opds.clone())?;
        if self.authorization.is_some()
            && let Some(shelf) = feed.shelf.as_ref()
            && shelf.origin() == self.opds.origin()
        {
            // Opening the account feed establishes the session used by the alternate search.
            let (response, _) = self.get(shelf.clone(), ATOM, true)?;
            ensure!(
                response
                    .headers()
                    .get("content-type")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|kind| kind.contains("xml")),
                "Флибуста вернула страницу входа вместо каталога"
            );
        }

        let template = if let Some(template) = feed.direct_search {
            SearchTemplate { base, template }
        } else if let Some(descriptor) = feed.search_descriptor {
            let (xml, base) = self.read_xml(descriptor)?;
            SearchTemplate {
                base,
                template: opensearch_template(&xml)?,
            }
        } else {
            let path = base.path().trim_end_matches('/');
            SearchTemplate {
                template: format!("{path}/search?searchTerm={{searchTerms}}"),
                base,
            }
        };
        *self.search.lock().expect("Flibusta search lock poisoned") = Some(template);
        Ok(feed.page)
    }

    pub fn page(&self, address: &str) -> Result<Page> {
        self.read_page(self.resolve(address)?)
    }

    pub fn more(&self, next: &NextPage) -> Result<Page> {
        self.page(
            next.flibusta_url()
                .context("Некорректная ссылка на страницу Флибусты")?,
        )
    }

    pub fn search(&self, mode: SearchMode, term: &str) -> Result<Page> {
        let term = term.trim();
        ensure!(!term.is_empty(), "Введи поисковый запрос");
        let kind = match mode {
            SearchMode::Books => "books",
            SearchMode::Authors => "authors",
        };
        let template = self
            .search
            .lock()
            .expect("Flibusta search lock poisoned")
            .clone();
        let template = match template {
            Some(template) => template,
            None => {
                self.root()?;
                self.search
                    .lock()
                    .expect("Flibusta search lock poisoned")
                    .clone()
                    .context("Не удалось найти поиск Флибусты")?
            }
        };
        let page = self.read_page(search_address(&template, term, kind)?)?;
        if let Some(link) = page
            .entries
            .iter()
            .filter_map(|entry| entry.navigation.as_deref())
            .filter_map(|link| Url::parse(link).ok())
            .find(|link| {
                link.query_pairs()
                    .any(|(key, value)| key == "searchType" && value == kind)
            })
        {
            return self.read_page(link);
        }
        Ok(page)
    }

    pub fn books_by_author(&self, author: &str) -> Result<Page> {
        let page = self.search(SearchMode::Authors, author)?;
        let author = author.trim().to_lowercase();
        if let Some(link) = page
            .entries
            .iter()
            .find(|entry| entry.title.trim().to_lowercase() == author)
            .and_then(|entry| entry.navigation.as_deref())
        {
            return self.page(link);
        }
        Ok(page)
    }

    pub fn detail(&self, entry: &Entry) -> Result<Entry> {
        let Some(detail) = entry.detail.as_deref() else {
            return Ok(entry.clone());
        };
        let (xml, url) = self.read_xml(self.resolve(detail)?)?;
        let document = Document::parse(&xml).context("Некорректная карточка Флибусты")?;
        let node = document.root_element();
        let node = if node.has_tag_name("entry") {
            node
        } else {
            child(node, "entry").context("Флибуста не вернула карточку книги")?
        };
        let Some(full) = parse_entry(node, &url, &[]) else {
            return Ok(entry.clone());
        };
        let mut combined = entry.clone();
        if !full.description.is_empty() {
            combined.description = full.description;
        }
        if !full.extra_metadata.is_empty() {
            combined.extra_metadata = full.extra_metadata;
        }
        Ok(combined)
    }

    pub fn cover(&self, address: &str) -> Result<Vec<u8>> {
        let (mut response, _) = self.get(self.resolve(address)?, "image/*", false)?;
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
        let (response, _) = self.get(self.resolve(&download.url)?, "*/*", true)?;
        save_book_response(response, download, entry, directory, progress)
    }

    fn read_page(&self, mut url: Url) -> Result<Page> {
        let mut visited = HashSet::new();
        for index in 0..MAX_FILTERED_PAGES {
            visited.insert(url.as_str().to_owned());
            let (mut feed, final_url) = self.read_feed(url)?;
            visited.insert(final_url.as_str().to_owned());
            if feed
                .page
                .next
                .as_ref()
                .and_then(NextPage::flibusta_url)
                .is_some_and(|next| visited.contains(next))
            {
                feed.page.next = None;
            }
            if !feed.page.entries.is_empty() || index + 1 == MAX_FILTERED_PAGES {
                return Ok(feed.page);
            }
            let Some(next) = feed.page.next.as_ref().and_then(NextPage::flibusta_url) else {
                return Ok(feed.page);
            };
            url = self.resolve(next)?;
        }
        unreachable!("MAX_FILTERED_PAGES is positive")
    }

    fn read_feed(&self, url: Url) -> Result<(Feed, Url)> {
        let (xml, url) = self.read_xml(url)?;
        let feed = parse_feed(&xml, &url, &self.languages)?;
        Ok((feed, url))
    }

    fn read_xml(&self, url: Url) -> Result<(String, Url)> {
        let (mut response, url) = self.get(
            url,
            "application/atom+xml, application/opensearchdescription+xml, application/xml",
            true,
        )?;
        let xml = response
            .body_mut()
            .with_config()
            .limit(XML_LIMIT)
            .read_to_string()?;
        Ok((xml, url))
    }

    fn resolve(&self, address: &str) -> Result<Url> {
        secure_url(self.opds.join(address)?, "https")
    }

    fn get(
        &self,
        mut url: Url,
        accept: &str,
        authenticated: bool,
    ) -> Result<(Response<ureq::Body>, Url)> {
        for _ in 0..=MAX_REDIRECTS {
            let mut request = self.http.get(url.as_str()).header(ACCEPT, accept);
            if authenticated
                && url.origin() == self.opds.origin()
                && let Some(authorization) = &self.authorization
            {
                request = request.header(AUTHORIZATION, authorization.clone());
            }
            let response = request.call().map_err(|error| {
                if matches!(error, ureq::Error::StatusCode(401)) {
                    anyhow::Error::new(error).context("Флибуста не приняла данные для входа")
                } else {
                    anyhow::Error::new(error).context("Запрос к Флибусте")
                }
            })?;
            if !response.status().is_redirection() {
                return Ok((response, url));
            }
            let location = response
                .headers()
                .get(LOCATION)
                .context("Флибуста вернула перенаправление без адреса")?
                .to_str()?;
            url = secure_url(url.join(location)?, "https")?;
        }
        bail!("Слишком много перенаправлений Флибусты")
    }
}

fn search_address(search: &SearchTemplate, term: &str, kind: &str) -> Result<Url> {
    let encoded: String = url::form_urlencoded::byte_serialize(term.as_bytes()).collect();
    let address = search
        .template
        .replace("{searchTerms?}", &encoded)
        .replace("{searchTerms}", &encoded)
        .replace("{startPage?}", "0")
        .replace("{startPage}", "0")
        .replace("{startIndex?}", "0")
        .replace("{count?}", "20")
        .replace("{language?}", "");
    let mut url = secure_url(search.base.join(&address)?, "https")?;
    let pairs: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key != "searchType")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.query_pairs_mut()
        .clear()
        .extend_pairs(pairs)
        .append_pair("searchType", kind);
    Ok(url)
}

fn opensearch_template(xml: &str) -> Result<String> {
    let document = Document::parse(xml).context("Некорректное описание поиска Флибусты")?;
    document
        .root_element()
        .children()
        .filter(|node| node.has_tag_name("Url"))
        .filter(|node| {
            node.attribute("type")
                .is_some_and(|kind| kind.starts_with(ATOM))
        })
        .filter_map(|node| node.attribute("template"))
        .find(|template| template.contains("{searchTerms"))
        .map(str::to_owned)
        .context("Флибуста не объявила адрес поиска")
}

fn parse_feed(xml: &str, base: &Url, languages: &[String]) -> Result<Feed> {
    let document = Document::parse(xml).context("Некорректный каталог Флибусты")?;
    let feed = document.root_element();
    ensure!(
        feed.has_tag_name("feed"),
        "Флибуста вернула страницу вместо каталога"
    );
    let entries: Vec<_> = feed
        .children()
        .filter(|node| node.has_tag_name("entry"))
        .collect();
    let shelf = entries
        .iter()
        .filter_map(|entry| navigation_link(*entry, base))
        .find(|url| url.path().trim_end_matches('/').ends_with("/opds/polka"));
    let entries = entries
        .into_iter()
        .filter_map(|entry| parse_entry(entry, base, languages))
        .collect();
    let next = links(feed)
        .find(|link| relation(*link, "next"))
        .and_then(|link| link_url(link, base))
        .map(|url| NextPage::flibusta(url.into()));
    let direct_search = links(feed)
        .filter(|link| relation(*link, "search") && mime(*link).starts_with(ATOM))
        .filter_map(|link| link.attribute("href"))
        .find(|href| href.contains("{searchTerms"))
        .map(str::to_owned);
    let search_descriptor = links(feed)
        .find(|link| relation(*link, "search") && mime(*link).starts_with(OPENSEARCH))
        .and_then(|link| link_url(link, base));
    Ok(Feed {
        page: Page { entries, next },
        direct_search,
        search_descriptor,
        shelf,
    })
}

fn parse_entry(node: Node<'_, '_>, base: &Url, languages: &[String]) -> Option<Entry> {
    let title = field(node, "title");
    if title.is_empty() {
        return None;
    }
    let detail = links(node)
        .find(|link| {
            (relation(*link, "alternate") || relation(*link, "self"))
                && mime(*link).starts_with(ATOM)
                && mime(*link).contains("type=entry")
        })
        .and_then(|link| link_url(link, base))
        .map(Url::into);
    if !links(node).any(is_acquisition) && detail.is_none() {
        let url = navigation_link(node, base)?;
        if is_series(&url, title) {
            return None;
        }
        let url = author_books_url(&url).unwrap_or(url);
        return Some(Entry {
            title: title.to_owned(),
            navigation: Some(url.into()),
            ..Entry::default()
        });
    }

    let content = child(node, "content").or_else(|| child(node, "summary"));
    let content = content.map_or_else(String::new, content_text);
    let mut source_format = field(node, "format");
    let mut language = field(node, "language");
    let mut series = "";
    let mut description = Vec::new();
    let mut edition = Vec::new();
    let issued = field(node, "issued");
    if !issued.is_empty() {
        edition.push(format!("Год издания: {issued}"));
    }
    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let (label, value) = line.split_once(':').unwrap_or_default();
        let value = value.trim();
        match label.to_lowercase().as_str() {
            "язык" => {
                if language.is_empty() {
                    language = value;
                }
            }
            "серия" => series = value,
            "перевод" | "переводчик" | "переводчики" if !value.is_empty() =>
            {
                edition.push(format!("Переводчики: {value}"));
            }
            "год издания" => {
                if issued.is_empty() && !value.is_empty() {
                    edition.push(line.to_owned());
                }
            }
            "формат" => {
                if source_format.is_empty() {
                    source_format = value;
                }
            }
            "скачиваний" => {}
            "размер" => edition.push(line.to_owned()),
            _ => description.push(line),
        }
    }
    if !language_allowed(language, languages) {
        return None;
    }
    let downloads: Vec<_> = original_download(node, base, source_format)
        .into_iter()
        .collect();
    if downloads.is_empty() && detail.is_none() {
        return None;
    }

    let mut authors = Vec::new();
    let mut author_links = Vec::new();
    for author in node.children().filter(|child| child.has_tag_name("author")) {
        let name = field(author, "name");
        if name.is_empty() {
            continue;
        }
        authors.push(name);
        let uri = field(author, "uri");
        let url = author_from_uri(base, uri)
            .map(String::from)
            .unwrap_or_default();
        author_links.push((name.to_owned(), url));
    }
    for link in
        links(node).filter(|link| relation(*link, "related") && mime(*link).starts_with(ATOM))
    {
        let Some(name) = link
            .attribute("title")
            .and_then(|title| title.strip_prefix("Все книги автора "))
        else {
            continue;
        };
        if let Some((_, address)) = author_links
            .iter_mut()
            .find(|(author, url)| author == name && url.is_empty())
            && let Some(url) = link_url(link, base).and_then(|url| author_books_url(&url))
        {
            *address = url.into();
        }
    }
    let genres: Vec<_> = node
        .children()
        .filter(|child| child.has_tag_name("category"))
        .filter_map(|category| {
            category
                .attribute("label")
                .or_else(|| category.attribute("term"))
        })
        .filter(|genre| !genre.is_empty())
        .collect();
    let mut metadata = Vec::new();
    for (label, value) in [
        ("Язык книги", language),
        ("Серия", series),
        ("Жанры", &genres.join(", ")),
    ] {
        if !value.is_empty() {
            metadata.push(format!("{label}: {value}"));
        }
    }
    let cover = links(node)
        .find(|link| {
            relation(*link, "http://opds-spec.org/image")
                || relation(*link, "http://opds-spec.org/cover")
                || relation(*link, "x-stanza-cover-image")
        })
        .and_then(|link| link_url(link, base))
        .map(Url::into);
    let thumbnail = links(node)
        .find(|link| {
            relation(*link, "http://opds-spec.org/image/thumbnail")
                || relation(*link, "http://opds-spec.org/thumbnail")
                || relation(*link, "x-stanza-cover-image-thumbnail")
        })
        .and_then(|link| link_url(link, base))
        .map(Url::into);
    Some(Entry {
        title: title.to_owned(),
        author: authors.join(", "),
        author_links,
        language: language.to_owned(),
        series: series.to_owned(),
        description: description.join("\n\n").chars().take(6000).collect(),
        metadata: metadata.join("\n"),
        extra_metadata: edition.join("\n"),
        detail,
        cover,
        thumbnail,
        downloads,
        ..Entry::default()
    })
}

fn is_acquisition(link: Node<'_, '_>) -> bool {
    link.attribute("rel")
        .unwrap_or_default()
        .split_ascii_whitespace()
        .any(|rel| {
            matches!(
                rel,
                "http://opds-spec.org/acquisition"
                    | "http://opds-spec.org/acquisition/open-access"
                    | "download"
            )
        })
}

fn original_download(node: Node<'_, '_>, base: &Url, source_format: &str) -> Option<Download> {
    let links: Vec<_> = links(node)
        .filter(|link| is_acquisition(*link))
        .filter_map(|link| acquisition(link, base))
        .collect();
    if let Some(link) = links
        .iter()
        .find(|(url, _)| url.path().ends_with("/download"))
    {
        return Some(link.1.clone());
    }
    let format = source_format.trim().rsplit('/').next().unwrap_or_default();
    let format = format.split(['+', '.']).next().unwrap_or_default();
    let format = match format {
        "x-fictionbook" => "fb2",
        "x-mobipocket-ebook" => "mobi",
        "vnd.djvu" => "djvu",
        _ => format,
    };
    if !format.is_empty() {
        return links
            .into_iter()
            .map(|(_, download)| download)
            .find(|download| {
                download
                    .extension
                    .split('.')
                    .next()
                    .unwrap_or_default()
                    .eq_ignore_ascii_case(format)
            });
    }
    // Older Flibusta feeds may omit the format; a single advertised link is unambiguous.
    if links.len() == 1 {
        return links.into_iter().next().map(|(_, download)| download);
    }
    None
}

fn acquisition(link: Node<'_, '_>, base: &Url) -> Option<(Url, Download)> {
    let (label, extension) = match mime(link).split(';').next()?.trim() {
        "application/fb2+zip" | "application/x-fictionbook+zip" => ("FB2", "fb2.zip"),
        "application/fb2+xml" | "application/x-fictionbook+xml" => ("FB2", "fb2"),
        "application/epub+zip" => ("EPUB", "epub"),
        "application/pdf" => ("PDF", "pdf"),
        "application/x-mobipocket-ebook" => ("MOBI", "mobi"),
        "image/vnd.djvu" | "image/x-djvu" => ("DJVU", "djvu"),
        _ => return None,
    };
    let url = link_url(link, base)?;
    let download = Download {
        label: label.into(),
        extension: extension.into(),
        url: url.to_string(),
    };
    Some((url, download))
}

fn navigation_link(node: Node<'_, '_>, base: &Url) -> Option<Url> {
    links(node)
        .filter(|link| mime(*link).starts_with(ATOM))
        .filter(|link| {
            let rel = link.attribute("rel").unwrap_or_default();
            rel.is_empty()
                || relation(*link, "subsection")
                || relation(*link, "http://opds-spec.org/subsection")
                || relation(*link, "http://opds-spec.org/sort/new")
                || relation(*link, "http://opds-spec.org/sort/popular")
        })
        .find_map(|link| link_url(link, base))
}

fn is_series(url: &Url, title: &str) -> bool {
    url.path_segments().is_some_and(|mut segments| {
        segments.any(|segment| {
            matches!(
                segment,
                "sequencesindex"
                    | "sequence"
                    | "sequences"
                    | "authorsequences"
                    | "authorsequenceless"
            )
        })
    }) || matches!(title, "По сериям" | "Серии")
}

fn author_from_uri(base: &Url, uri: &str) -> Option<Url> {
    let uri = secure_url(base.join(uri).ok()?, "https").ok()?;
    if uri.origin() != base.origin() {
        return None;
    }
    if let Some(id) = uri.path().strip_prefix("/a/").filter(|id| is_id(id)) {
        return uri.join(&format!("/opds/author/{id}/alphabet/0")).ok();
    }
    author_books_url(&uri)
}

fn author_books_url(url: &Url) -> Option<Url> {
    let mut segments = url.path_segments()?;
    if segments.next()? != "opds" || segments.next()? != "author" {
        return None;
    }
    let id = segments.next().filter(|id| is_id(id))?;
    if segments.any(|segment| !segment.is_empty()) {
        return None;
    }
    url.join(&format!("/opds/author/{id}/alphabet/0")).ok()
}

fn is_id(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn content_text(content: Node<'_, '_>) -> String {
    if content.children().any(|child| child.is_element()) {
        return content
            .descendants()
            .filter_map(|child| {
                if child.is_text() {
                    child.text()
                } else if matches!(child.tag_name().name(), "p" | "div" | "br" | "li") {
                    Some("\n")
                } else {
                    None
                }
            })
            .collect();
    }
    let content_type = content.attribute("type").unwrap_or_default();
    let value = content.text().unwrap_or_default();
    if !matches!(content_type, "html" | "text/html") {
        return value.to_owned();
    }
    let mut text = String::new();
    let mut remaining = value;
    while let Some((before, after)) = remaining.split_once('<') {
        text.push_str(before);
        let Some((tag, rest)) = after.split_once('>') else {
            text.push('<');
            remaining = after;
            break;
        };
        let tag = tag
            .trim_start_matches('/')
            .split(|ch: char| ch.is_whitespace() || ch == '/')
            .next()
            .unwrap_or_default();
        if matches!(tag.to_ascii_lowercase().as_str(), "p" | "div" | "br" | "li") {
            text.push('\n');
        }
        remaining = rest;
    }
    text.push_str(remaining);
    decode_entities(&text)
}

fn child<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Option<Node<'a, 'input>> {
    node.children().find(|child| child.has_tag_name(name))
}

fn field<'a>(node: Node<'a, '_>, name: &str) -> &'a str {
    child(node, name)
        .and_then(|child| child.text())
        .unwrap_or_default()
        .trim()
}

fn links<'a, 'input>(node: Node<'a, 'input>) -> impl Iterator<Item = Node<'a, 'input>> {
    node.children().filter(|child| child.has_tag_name("link"))
}

fn relation(node: Node<'_, '_>, expected: &str) -> bool {
    node.attribute("rel")
        .unwrap_or_default()
        .split_ascii_whitespace()
        .any(|rel| rel == expected)
}

fn mime<'a>(node: Node<'a, '_>) -> &'a str {
    node.attribute("type").unwrap_or_default()
}

fn link_url(node: Node<'_, '_>, base: &Url) -> Option<Url> {
    secure_url(base.join(node.attribute("href")?).ok()?, "https").ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_keeps_original_format_and_native_author_links_after_language_filtering() {
        let url = Url::parse("https://catalog.example/opds/new").unwrap();
        let xml = r#"
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/terms/">
                <link rel="next" href="?page=2"/>
                <entry>
                    <title>Пример</title>
                    <author><name>Первый, Автор</name><uri>/a/12</uri></author>
                    <author><name>Второй Автор</name></author>
                    <category term="fiction" label="Проза"/>
                    <dc:format>fb2+zip</dc:format>
                    <content type="text/html">&lt;p&gt;Аннотация &amp;amp; пример.&lt;/p&gt;
                    &lt;br/&gt;Язык: ru&lt;br/&gt;Перевод: Переводчик&lt;br/&gt;Серия: Пример серии</content>
                    <link rel="http://opds-spec.org/acquisition/open-access" type="application/epub+zip" href="/b/1/epub"/>
                    <link rel="http://opds-spec.org/acquisition/open-access" type="application/fb2+zip" href="/b/1/fb2"/>
                </entry>
                <entry>
                    <title>Autre langue</title>
                    <dc:language>fr</dc:language>
                    <link rel="http://opds-spec.org/acquisition/open-access" type="application/epub+zip" href="/b/2/epub"/>
                </entry>
                <entry>
                    <title>По сериям</title>
                    <link type="application/atom+xml" href="/opds/sequencesindex"/>
                </entry>
            </feed>
        "#;
        let feed = parse_feed(xml, &url, &["en".into(), "ru".into()]).unwrap();
        assert_eq!(feed.page.entries.len(), 1);
        let book = &feed.page.entries[0];
        assert_eq!(book.downloads.len(), 1);
        assert_eq!(book.downloads[0].extension, "fb2.zip");
        assert_eq!(book.downloads[0].url, "https://catalog.example/b/1/fb2");
        assert_eq!(
            book.author_links,
            [
                (
                    "Первый, Автор".into(),
                    "https://catalog.example/opds/author/12/alphabet/0".into()
                ),
                ("Второй Автор".into(), String::new()),
            ]
        );
        assert_eq!(book.description, "Аннотация & пример.");
        assert_eq!(book.extra_metadata, "Переводчики: Переводчик");
        assert_eq!(book.series, "Пример серии");
        assert!(book.metadata.starts_with("Язык книги: ru\n"));
        assert!(book.metadata.contains("Серия: Пример серии"));
        assert_eq!(
            feed.page.next.as_ref().and_then(NextPage::flibusta_url),
            Some("https://catalog.example/opds/new?page=2")
        );
    }

    #[test]
    fn search_template_preserves_encoded_term_and_replaces_book_mode() {
        let template = SearchTemplate {
            base: Url::parse("https://catalog.example/opds").unwrap(),
            template:
                "/opds/search?searchTerm={searchTerms}&searchType=books&pageNumber={startPage?}"
                    .into(),
        };
        let url = search_address(&template, "Имя & фамилия", "authors").unwrap();
        assert_eq!(
            url.query_pairs().into_owned().collect::<Vec<_>>(),
            [
                ("searchTerm".to_owned(), "Имя & фамилия".to_owned()),
                ("pageNumber".to_owned(), "0".to_owned()),
                ("searchType".to_owned(), "authors".to_owned()),
            ]
        );
    }
}
