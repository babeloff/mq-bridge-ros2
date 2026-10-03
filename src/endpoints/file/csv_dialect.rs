//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! The CSV dialect a `file` or `object_store` endpoint reads and writes.

use crate::models::{CsvConfig, CsvNested, FileFormat};
use anyhow::{anyhow, bail};
use std::sync::{Arc, OnceLock};

/// What `separator: auto` chooses between, in order of preference on a tie.
const AUTO_CANDIDATES: [u8; 4] = *b",;\t|";

/// The bytes that carry CSV syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CsvSyntax {
    pub(crate) separator: u8,
    /// `None` reads and writes every field unquoted.
    pub(crate) quote: Option<u8>,
}

impl Default for CsvSyntax {
    fn default() -> Self {
        Self {
            separator: b',',
            quote: Some(b'"'),
        }
    }
}

#[derive(Debug, Clone)]
enum Separator {
    Fixed(u8),
    /// Guessed from the first record; shared so every reader of one source agrees.
    Auto(Arc<OnceLock<u8>>),
}

#[derive(Debug, Clone)]
pub(crate) struct CsvDialect {
    separator: Separator,
    pub(crate) quote: Option<u8>,
    /// Whether the first record names the columns.
    pub(crate) header: bool,
    /// Column names from the config; they take the place of the header's.
    pub(crate) columns: Option<Arc<[String]>>,
    /// (Sink) Nested objects become `parent.child` columns instead of JSON text.
    pub(crate) flatten: bool,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            separator: Separator::Fixed(b','),
            quote: Some(b'"'),
            header: true,
            columns: None,
            flatten: true,
        }
    }
}

impl CsvDialect {
    /// The dialect of an endpoint; the default one when `format` is not CSV.
    pub(crate) fn for_format(
        format: &FileFormat,
        config: &CsvConfig,
        delimiter: &[u8],
    ) -> anyhow::Result<Self> {
        match format {
            FileFormat::Csv => Self::from_config(config, delimiter),
            _ => Ok(Self::default()),
        }
    }

    /// `delimiter` is the record separator, which the field syntax must stay clear of.
    pub(crate) fn from_config(config: &CsvConfig, delimiter: &[u8]) -> anyhow::Result<Self> {
        let quote = match config.quote.as_deref() {
            Some("none") => None,
            Some(value) => Some(parse_byte(value, "quote")?),
            None => Some(b'"'),
        };
        let (separator, separators): (_, &[u8]) = match config.separator.as_deref() {
            Some("auto") => (Separator::Auto(Arc::new(OnceLock::new())), &AUTO_CANDIDATES),
            Some(value) => (Separator::Fixed(parse_byte(value, "separator")?), &[]),
            None => (Separator::Fixed(b','), &[]),
        };
        let fixed = match separator {
            Separator::Fixed(byte) => Some(byte),
            Separator::Auto(_) => None,
        };
        let mut syntax = separators.iter().copied().chain(fixed).chain(quote);
        if quote.is_some_and(|q| fixed == Some(q) || separators.contains(&q)) {
            bail!("csv: `separator` and `quote` must differ");
        }
        if syntax.any(|byte| delimiter.contains(&byte)) {
            bail!("csv: the record `delimiter` must not contain the `separator` or `quote`");
        }
        let mut seen = std::collections::HashSet::new();
        if let Some(name) = config.columns.iter().find(|name| !seen.insert(*name)) {
            bail!("csv: `columns` repeats '{name}'");
        }
        Ok(Self {
            separator,
            quote,
            header: config.header.unwrap_or(true),
            columns: (!config.columns.is_empty()).then(|| config.columns.as_slice().into()),
            flatten: config.nested == CsvNested::Flatten,
        })
    }

    /// A source has nothing else to name its columns by.
    pub(crate) fn check_source(&self) -> anyhow::Result<()> {
        if !self.header && self.columns.is_none() {
            bail!("csv: a source with `header: false` needs `columns`");
        }
        Ok(())
    }

    /// A sink has no record to guess the separator from.
    pub(crate) fn check_sink(&self) -> anyhow::Result<()> {
        if matches!(self.separator, Separator::Auto(_)) {
            bail!("csv: `separator: auto` is for sources; a sink needs the separator itself");
        }
        Ok(())
    }

    /// Whether a sink appending to a file takes its columns from the header already there.
    pub(crate) fn reads_header_back(&self) -> bool {
        self.header && self.columns.is_none()
    }

    /// The syntax to read or write with. An `auto` separator still undecided reads as `,`.
    pub(crate) fn syntax(&self) -> CsvSyntax {
        let separator = match &self.separator {
            Separator::Fixed(byte) => *byte,
            Separator::Auto(found) => found.get().copied().unwrap_or(b','),
        };
        CsvSyntax {
            separator,
            quote: self.quote,
        }
    }

    /// [`Self::syntax`], settling an `auto` separator on `first_record` if it is still open.
    pub(crate) fn resolve(&self, first_record: &[u8]) -> CsvSyntax {
        if let Separator::Auto(found) = &self.separator {
            found.get_or_init(|| {
                let separator = guess_separator(first_record, self.quote);
                tracing::info!(separator = %(separator as char).escape_default(), "CSV separator detected");
                separator
            });
        }
        self.syntax()
    }
}

/// The candidate that occurs most often outside quotes; `,` when none does.
fn guess_separator(record: &[u8], quote: Option<u8>) -> u8 {
    let mut counts = [0usize; AUTO_CANDIDATES.len()];
    let mut in_quotes = false;
    for &byte in record {
        if Some(byte) == quote {
            in_quotes = !in_quotes;
        } else if !in_quotes {
            if let Some(i) = AUTO_CANDIDATES.iter().position(|&c| c == byte) {
                counts[i] += 1;
            }
        }
    }
    let mut best = 0;
    for (i, &count) in counts.iter().enumerate() {
        if count > counts[best] {
            best = i;
        }
    }
    AUTO_CANDIDATES[best]
}

/// One ASCII byte: a character, a name (`tab`, `space`, …) or hex like `0x1f`.
fn parse_byte(value: &str, what: &str) -> anyhow::Result<u8> {
    let invalid = || {
        anyhow!("csv: `{what}` must be one ASCII character, `tab`, `space` or hex like 0x1f, got '{value}'")
    };
    let byte = match value {
        "tab" | "\\t" => b'\t',
        "space" => b' ',
        "comma" => b',',
        "semicolon" => b';',
        "pipe" => b'|',
        hex if hex.len() == 4 && hex.starts_with("0x") => {
            u8::from_str_radix(&hex[2..], 16).map_err(|_| invalid())?
        }
        one if one.len() == 1 => one.as_bytes()[0],
        _ => return Err(invalid()),
    };
    if !byte.is_ascii() || matches!(byte, b'\n' | b'\r') {
        return Err(invalid());
    }
    Ok(byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialect(separator: &str, quote: Option<&str>) -> anyhow::Result<CsvDialect> {
        let config = CsvConfig {
            separator: Some(separator.to_string()),
            quote: quote.map(str::to_string),
            ..Default::default()
        };
        CsvDialect::from_config(&config, b"\n")
    }

    #[test]
    fn separator_takes_characters_names_and_hex() {
        for (spelling, byte) in [
            (";", b';'),
            ("tab", b'\t'),
            ("\\t", b'\t'),
            ("\t", b'\t'),
            ("space", b' '),
            ("pipe", b'|'),
            ("0x1f", 0x1f),
        ] {
            assert_eq!(
                dialect(spelling, None).unwrap().syntax().separator,
                byte,
                "{spelling}"
            );
        }
        assert_eq!(dialect("tab", Some("none")).unwrap().quote, None);
        assert_eq!(dialect(",", Some("'")).unwrap().quote, Some(b'\''));
    }

    #[test]
    fn rejects_what_the_parser_cannot_tell_apart() {
        assert!(dialect(";;", None).is_err());
        assert!(dialect("ä", None).is_err());
        assert!(dialect("0x0a", None).is_err());
        assert!(dialect("\"", None).is_err());
        let csv = CsvConfig {
            separator: Some("|".to_string()),
            ..Default::default()
        };
        assert!(CsvDialect::from_config(&csv, b"|\n").is_err());
        let csv = CsvConfig {
            columns: vec!["a".to_string(), "a".to_string()],
            ..Default::default()
        };
        assert!(CsvDialect::from_config(&csv, b"\n").is_err());
    }

    #[test]
    fn a_source_without_a_header_needs_columns() {
        let mut csv = CsvConfig {
            header: Some(false),
            ..Default::default()
        };
        let headless = CsvDialect::from_config(&csv, b"\n").unwrap();
        assert!(headless.check_source().is_err());
        csv.columns = vec!["id".to_string()];
        assert!(CsvDialect::from_config(&csv, b"\n")
            .unwrap()
            .check_source()
            .is_ok());
    }

    #[test]
    fn auto_picks_the_most_frequent_candidate_outside_quotes() {
        for (first_record, separator) in [
            ("id;name;city\r\n", b';'),
            ("id\tname\tcity\n", b'\t'),
            ("id|name\n", b'|'),
            ("\"a;b;c\",d,e\n", b','),
            ("single\n", b','),
        ] {
            let auto = dialect("auto", None).unwrap();
            assert!(auto.check_sink().is_err());
            assert_eq!(
                auto.resolve(first_record.as_bytes()).separator,
                separator,
                "{first_record:?}"
            );
            // Settled: a later record cannot change it.
            assert_eq!(auto.resolve(b"a,b,c,d,e,f").separator, separator);
        }
    }
}
