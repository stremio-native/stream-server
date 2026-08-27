use super::{
    github::{GitHubRelease, fetch_releases, make_client},
    version::{
        UpdateChannel, current_version, is_newer, normalize_current_version, parse_release_tag,
    },
};
use regex::Regex;
use semver::Version;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

const CHANGELOG_CACHE_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangelogResponse {
    pub current_version: String,
    pub latest_version: Option<String>,
    pub tag: Option<String>,
    pub release_url: Option<String>,
    pub published_at: Option<String>,
    pub channel: UpdateChannel,
    pub state: ChangelogState,
    pub sections: Vec<ChangelogSection>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChangelogState {
    Loaded,
    UpToDate,
    Offline,
    NotFound,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangelogSection {
    pub title: String,
    pub items: Vec<String>,
}

#[derive(Clone)]
struct CachedChangelog {
    fetched_at: Instant,
    response: ChangelogResponse,
}

static CHANGELOG_CACHE: OnceLock<RwLock<HashMap<&'static str, CachedChangelog>>> = OnceLock::new();
static MARKDOWN_LINK_RE: OnceLock<Regex> = OnceLock::new();

pub async fn latest_changelog(channel: UpdateChannel, force: bool) -> ChangelogResponse {
    if !force && let Some(response) = cached_response(channel).await {
        return response;
    }

    let response = fetch_latest_changelog(channel).await;
    if matches!(
        response.state,
        ChangelogState::Loaded | ChangelogState::UpToDate | ChangelogState::NotFound
    ) {
        cache_response(channel, response.clone()).await;
    }
    response
}

async fn cached_response(channel: UpdateChannel) -> Option<ChangelogResponse> {
    let cache = CHANGELOG_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    let cache = cache.read().await;
    let cached = cache.get(cache_key(channel))?;
    if cached.fetched_at.elapsed() <= CHANGELOG_CACHE_TTL {
        Some(cached.response.clone())
    } else {
        None
    }
}

async fn cache_response(channel: UpdateChannel, response: ChangelogResponse) {
    let cache = CHANGELOG_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    cache.write().await.insert(
        cache_key(channel),
        CachedChangelog {
            fetched_at: Instant::now(),
            response,
        },
    );
}

async fn fetch_latest_changelog(channel: UpdateChannel) -> ChangelogResponse {
    let client = match make_client() {
        Ok(client) => client,
        Err(err) => return error_response(channel, ChangelogState::Error, err.to_string()),
    };

    let releases = match fetch_releases(&client).await {
        Ok(releases) => releases,
        Err(err) => return error_response(channel, ChangelogState::Offline, err.to_string()),
    };

    let Some((version, release)) = select_latest_changelog_release(&releases, channel) else {
        return ChangelogResponse {
            current_version: current_version().to_string(),
            latest_version: None,
            tag: None,
            release_url: None,
            published_at: None,
            channel,
            state: ChangelogState::NotFound,
            sections: Vec::new(),
            error: Some("no eligible GitHub release was found".to_string()),
        };
    };

    let current = normalize_current_version();
    let body = release.body.as_deref().unwrap_or_default();
    ChangelogResponse {
        current_version: current_version().to_string(),
        latest_version: Some(release.tag_name.trim_start_matches('v').to_string()),
        tag: Some(release.tag_name.clone()),
        release_url: Some(release.html_url.clone()),
        published_at: release.published_at.clone(),
        channel,
        state: if is_newer(&version, &current) {
            ChangelogState::Loaded
        } else {
            ChangelogState::UpToDate
        },
        sections: parse_changelog_sections(body),
        error: None,
    }
}

fn error_response(
    channel: UpdateChannel, state: ChangelogState, message: String,
) -> ChangelogResponse {
    ChangelogResponse {
        current_version: current_version().to_string(),
        latest_version: None,
        tag: None,
        release_url: None,
        published_at: None,
        channel,
        state,
        sections: Vec::new(),
        error: Some(message),
    }
}

fn cache_key(channel: UpdateChannel) -> &'static str {
    match channel {
        UpdateChannel::Stable => "stable",
        UpdateChannel::Prerelease => "prerelease",
    }
}

fn select_latest_changelog_release(
    releases: &[GitHubRelease], channel: UpdateChannel,
) -> Option<(Version, GitHubRelease)> {
    let mut selected: Option<(Version, GitHubRelease)> = None;

    for release in releases {
        if release.draft || (channel == UpdateChannel::Stable && release.prerelease) {
            continue;
        }

        let Some(version) = parse_release_tag(&release.tag_name) else {
            continue;
        };

        let replace = selected
            .as_ref()
            .map(|(selected_version, _)| version > *selected_version)
            .unwrap_or(true);
        if replace {
            selected = Some((version, release.clone()));
        }
    }

    selected
}

pub fn parse_changelog_sections(body: &str) -> Vec<ChangelogSection> {
    let mut sections = Vec::new();
    let mut current_title: Option<String> = None;
    let mut current_items: Vec<String> = Vec::new();
    let mut skipping = false;

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('|') {
            continue;
        }

        if line == "---" {
            if skipping {
                skipping = false;
                current_title = None;
                current_items.clear();
            }
            continue;
        }

        if let Some(title) = heading_title(line) {
            push_section(&mut sections, current_title.take(), &mut current_items);
            skipping = title.eq_ignore_ascii_case("Downloads");
            current_title = if skipping {
                None
            } else {
                Some(title.to_string())
            };
            continue;
        }

        if skipping {
            continue;
        }

        if let Some(item) = bullet_item(line) {
            current_items.push(clean_markdown_text(item));
        } else if let Some(item) = special_note_item(line) {
            if current_title.is_none() {
                current_title = Some("Release Notes".to_string());
            }
            current_items.push(item);
        }
    }

    push_section(&mut sections, current_title, &mut current_items);

    if sections.is_empty() {
        let items = body
            .lines()
            .map(str::trim)
            .filter(|line| {
                !line.is_empty()
                    && *line != "---"
                    && !line.starts_with('|')
                    && heading_title(line).is_none()
            })
            .filter(|line| !looks_like_release_preamble(line))
            .map(clean_markdown_text)
            .filter(|line| {
                !line.is_empty()
                    && !line.eq_ignore_ascii_case("Verify your download against `SHA256SUMS.txt`.")
            })
            .collect::<Vec<_>>();

        if !items.is_empty() {
            sections.push(ChangelogSection {
                title: "Release Notes".to_string(),
                items,
            });
        }
    }

    sections
}

fn heading_title(line: &str) -> Option<&str> {
    line.strip_prefix("#### ")
        .or_else(|| line.strip_prefix("### "))
        .or_else(|| line.strip_prefix("## "))
        .map(str::trim)
        .filter(|title| !title.is_empty())
}

fn bullet_item(line: &str) -> Option<&str> {
    line.strip_prefix("- ")
        .or_else(|| line.strip_prefix("* "))
        .map(str::trim)
        .filter(|item| !item.is_empty())
}

fn push_section(
    sections: &mut Vec<ChangelogSection>, title: Option<String>, items: &mut Vec<String>,
) {
    let Some(title) = title else {
        items.clear();
        return;
    };
    if items.is_empty() {
        return;
    }
    sections.push(ChangelogSection {
        title,
        items: std::mem::take(items),
    });
}

fn special_note_item(line: &str) -> Option<String> {
    let lower = line.to_ascii_lowercase();
    if lower.starts_with("**full changelog**:")
        || lower.starts_with("**full comparison**:")
        || lower.starts_with("full changelog:")
        || lower.starts_with("full comparison:")
    {
        Some(clean_markdown_text(line))
    } else {
        None
    }
}

fn looks_like_release_preamble(line: &str) -> bool {
    line.starts_with("## Stream Server")
        || line.starts_with("High-performance torrent streaming server")
        || line.starts_with("Stremio-compatible streaming API")
        || line.starts_with("hardware-accelerated transcoding")
}

fn clean_markdown_text(input: &str) -> String {
    strip_markdown_links(input)
        .replace("**", "")
        .replace("__", "")
        .replace('`', "")
        .trim()
        .to_string()
}

fn strip_markdown_links(input: &str) -> String {
    let re = MARKDOWN_LINK_RE.get_or_init(|| Regex::new(r"\[([^\]]+)\]\([^)]+\)").unwrap());
    re.replace_all(input, "$1").into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(tag: &str, prerelease: bool, draft: bool) -> GitHubRelease {
        GitHubRelease {
            tag_name: tag.to_string(),
            html_url: format!("https://example.test/{tag}"),
            draft,
            prerelease,
            name: Some(tag.to_string()),
            body: Some("### Bug Fixes\n\n- Fixed playback".to_string()),
            published_at: Some("2026-01-01T00:00:00Z".to_string()),
            assets: Vec::new(),
        }
    }

    #[test]
    fn parser_skips_downloads_and_strips_links() {
        let sections = parse_changelog_sections(
            "### Downloads\n\n- [asset](https://example.test)\n\n### Bug Fixes\n\n- Fixed thing ([abc1234](https://example.test/commit))",
        );

        assert_eq!(
            sections,
            vec![ChangelogSection {
                title: "Bug Fixes".to_string(),
                items: vec!["Fixed thing (abc1234)".to_string()],
            }]
        );
    }

    #[test]
    fn parser_falls_back_to_release_notes() {
        let sections = parse_changelog_sections("Plain note\nAnother note");

        assert_eq!(
            sections,
            vec![ChangelogSection {
                title: "Release Notes".to_string(),
                items: vec!["Plain note".to_string(), "Another note".to_string()],
            }]
        );
    }

    #[test]
    fn parser_handles_old_full_changelog_only_body() {
        let sections = parse_changelog_sections(
            "## Stream Server v0.1.7.2\n\nHigh-performance torrent streaming server with HLS transcoding and a\n\n### Downloads\n\n| Platform | Download |\n| --- | --- |\n| Windows | [Download](https://example.test) |\n\nVerify your download against `SHA256SUMS.txt`.\n\n---\n\n### Changelog\n\n**Full Changelog**: https://github.com/perpetus/stream-server/compare/v0.1.7.1...v0.1.7.2",
        );

        assert_eq!(
            sections,
            vec![ChangelogSection {
                title: "Changelog".to_string(),
                items: vec![
                    "Full Changelog: https://github.com/perpetus/stream-server/compare/v0.1.7.1...v0.1.7.2".to_string()
                ],
            }]
        );
    }

    #[test]
    fn stable_channel_ignores_prereleases_for_changelog() {
        let releases = vec![
            release("v0.2.0-beta.1", true, false),
            release("v0.1.8", false, false),
        ];
        let (_, selected) =
            select_latest_changelog_release(&releases, UpdateChannel::Stable).unwrap();

        assert_eq!(selected.tag_name, "v0.1.8");
    }

    #[test]
    fn three_component_release_sorts_after_older_four_component_release() {
        let releases = vec![
            release("v0.1.7.2", false, false),
            release("v0.1.8", false, false),
        ];
        let (_, selected) =
            select_latest_changelog_release(&releases, UpdateChannel::Stable).unwrap();

        assert_eq!(selected.tag_name, "v0.1.8");
    }

    #[test]
    fn prerelease_channel_includes_prereleases_for_changelog() {
        let releases = vec![
            release("v0.2.0-beta.1", true, false),
            release("v0.1.8", false, false),
        ];
        let (_, selected) =
            select_latest_changelog_release(&releases, UpdateChannel::Prerelease).unwrap();

        assert_eq!(selected.tag_name, "v0.2.0-beta.1");
    }

    #[test]
    fn draft_releases_are_ignored() {
        let releases = vec![
            release("v0.9.0", false, true),
            release("v0.1.8", false, false),
        ];
        let (_, selected) =
            select_latest_changelog_release(&releases, UpdateChannel::Stable).unwrap();

        assert_eq!(selected.tag_name, "v0.1.8");
    }
}
