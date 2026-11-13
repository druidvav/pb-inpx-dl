pub mod catalog;
pub mod cover_cache;
mod flibusta;

use std::path::{Path, PathBuf};

use anyhow::Result;
use catalog::{Client, Config, Download, Entry, NextPage, Page, SearchMode};
use flibusta::FlibustaClient;

pub struct CatalogSource {
    client: Box<dyn CatalogBackend>,
    pub key: &'static str,
}

impl CatalogSource {
    pub fn inpx(config: Config) -> Result<Self> {
        Ok(Self {
            client: Box::new(Client::new(config)?),
            key: "inpx",
        })
    }

    pub fn flibusta(config: Config) -> Result<Self> {
        Ok(Self {
            client: Box::new(FlibustaClient::new(config.flibusta, config.languages)?),
            key: "flibusta",
        })
    }

    pub fn backend(&self) -> &dyn CatalogBackend {
        self.client.as_ref()
    }
}

pub trait CatalogBackend: Send + Sync {
    fn root(&self) -> Result<Page>;
    fn page(&self, address: &str) -> Result<Page>;
    fn more(&self, next: &NextPage) -> Result<Page>;
    fn search(&self, mode: SearchMode, term: &str) -> Result<Page>;
    fn books_by_author(&self, author: &str) -> Result<Page>;
    fn detail(&self, entry: &Entry) -> Result<Entry>;
    fn cover(&self, address: &str) -> Result<Vec<u8>>;
    fn download(
        &self,
        download: &Download,
        entry: &Entry,
        directory: &Path,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<PathBuf>;
}

impl CatalogBackend for Client {
    fn root(&self) -> Result<Page> {
        self.root()
    }
    fn page(&self, address: &str) -> Result<Page> {
        self.page(address)
    }
    fn more(&self, next: &NextPage) -> Result<Page> {
        self.more(next)
    }
    fn search(&self, mode: SearchMode, term: &str) -> Result<Page> {
        self.search(mode, term)
    }
    fn books_by_author(&self, author: &str) -> Result<Page> {
        self.books_by_author(author)
    }
    fn detail(&self, entry: &Entry) -> Result<Entry> {
        self.detail(entry)
    }
    fn cover(&self, address: &str) -> Result<Vec<u8>> {
        self.cover(address)
    }
    fn download(
        &self,
        download: &Download,
        entry: &Entry,
        directory: &Path,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<PathBuf> {
        self.download(download, entry, directory, progress)
    }
}

impl CatalogBackend for FlibustaClient {
    fn root(&self) -> Result<Page> {
        self.root()
    }
    fn page(&self, address: &str) -> Result<Page> {
        self.page(address)
    }
    fn more(&self, next: &NextPage) -> Result<Page> {
        self.more(next)
    }
    fn search(&self, mode: SearchMode, term: &str) -> Result<Page> {
        self.search(mode, term)
    }
    fn books_by_author(&self, author: &str) -> Result<Page> {
        self.books_by_author(author)
    }
    fn detail(&self, entry: &Entry) -> Result<Entry> {
        self.detail(entry)
    }
    fn cover(&self, address: &str) -> Result<Vec<u8>> {
        self.cover(address)
    }
    fn download(
        &self,
        download: &Download,
        entry: &Entry,
        directory: &Path,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<PathBuf> {
        self.download(download, entry, directory, progress)
    }
}
