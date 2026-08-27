use super::{
    assets::{RequiredAssets, select_required_assets},
    error::{UpdateError, UpdateErrorKind},
    state::UpdateCandidate,
    version::{UpdateChannel, is_newer, parse_release_tag},
};
use semver::Version;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRelease {
    pub tag_name: String,
    pub html_url: String,
    pub draft: bool,
    pub prerelease: bool,
    #[serde(default)]
    #[allow(dead_code)]
    pub name: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub published_at: Option<String>,
    pub assets: Vec<GitHubAsset>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubAsset {
    pub name: String,
    pub browser_download_url: String,
    #[serde(default)]
    pub size: u64,
}

pub async fn fetch_releases(client: &reqwest::Client) -> Result<Vec<GitHubRelease>, UpdateError> {
    let releases = client
        .get("https://api.github.com/repos/perpetus/stream-server/releases")
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<GitHubRelease>>()
        .await?;
    Ok(releases)
}

pub fn select_latest_candidate(
    releases: &[GitHubRelease], channel: UpdateChannel, current: &Version,
) -> Result<Option<UpdateCandidate>, UpdateError> {
    let mut selected: Option<(Version, UpdateCandidate)> = None;
    let mut last_asset_error: Option<UpdateError> = None;

    for release in releases {
        if release.draft {
            continue;
        }
        if channel == UpdateChannel::Stable && release.prerelease {
            continue;
        }

        let Some(version) = parse_release_tag(&release.tag_name) else {
            continue;
        };
        if !is_newer(&version, current) {
            continue;
        }

        let assets = match select_required_assets(&release.assets) {
            Ok(assets) => assets,
            Err(err) => {
                last_asset_error = Some(err);
                continue;
            }
        };

        let candidate = UpdateCandidate {
            version: release.tag_name.trim_start_matches('v').to_string(),
            tag: release.tag_name.clone(),
            release_url: release.html_url.clone(),
            prerelease: release.prerelease,
            assets,
        };

        let should_replace = selected
            .as_ref()
            .map(|(selected_version, _)| version > *selected_version)
            .unwrap_or(true);
        if should_replace {
            selected = Some((version, candidate));
        }
    }

    if let Some((_, candidate)) = selected {
        Ok(Some(candidate))
    } else if let Some(err) = last_asset_error {
        Err(UpdateError::new(
            UpdateErrorKind::InvalidRelease,
            format!("latest eligible release is missing required assets: {err}"),
        ))
    } else {
        Ok(None)
    }
}

pub fn make_client() -> Result<reqwest::Client, UpdateError> {
    reqwest::Client::builder()
        .user_agent(format!(
            "stream-server-updater/{}",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .map_err(UpdateError::from)
}

#[allow(dead_code)]
fn _assert_required_assets_are_send_sync(_: RequiredAssets) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updater::assets::{
        CHECKSUMS_ASSET_NAME, SERVER_WINDOWS_ASSET_NAME, STREMIO_RUNTIME_WINDOWS_ASSET_NAME,
        UPDATER_WINDOWS_ASSET_NAME,
    };

    fn asset(name: &str) -> GitHubAsset {
        GitHubAsset {
            name: name.to_string(),
            browser_download_url: format!("https://example.test/{name}"),
            size: 1,
        }
    }

    fn release(tag: &str, prerelease: bool) -> GitHubRelease {
        GitHubRelease {
            tag_name: tag.to_string(),
            html_url: format!("https://example.test/releases/{tag}"),
            draft: false,
            prerelease,
            name: None,
            body: None,
            published_at: None,
            assets: vec![
                asset(SERVER_WINDOWS_ASSET_NAME),
                asset(STREMIO_RUNTIME_WINDOWS_ASSET_NAME),
                asset(UPDATER_WINDOWS_ASSET_NAME),
                asset(CHECKSUMS_ASSET_NAME),
            ],
        }
    }

    #[test]
    fn stable_channel_ignores_prereleases() {
        let releases = vec![release("v0.2.0-beta.1", true), release("v0.1.3", false)];
        let selected = select_latest_candidate(
            &releases,
            UpdateChannel::Stable,
            &Version::parse("0.1.2").unwrap(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(selected.version, "0.1.3");
    }

    #[test]
    fn prerelease_channel_includes_beta_tags() {
        let releases = vec![release("v0.2.0-beta.1", true), release("v0.1.3", false)];
        let selected = select_latest_candidate(
            &releases,
            UpdateChannel::Prerelease,
            &Version::parse("0.1.2").unwrap(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(selected.version, "0.2.0-beta.1");
    }
}
