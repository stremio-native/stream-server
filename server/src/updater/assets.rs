use super::{
    error::{UpdateError, UpdateErrorKind},
    github::GitHubAsset,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::Path};
use tokio::io::AsyncWriteExt;

pub const SERVER_WINDOWS_ASSET_NAME: &str = "stream-server-windows-amd64.exe";
pub const STREMIO_RUNTIME_WINDOWS_ASSET_NAME: &str = "stremio-runtime-windows-amd64.exe";
pub const UPDATER_WINDOWS_ASSET_NAME: &str = "stream-server-updater-windows-amd64.exe";
pub const CHECKSUMS_ASSET_NAME: &str = "SHA256SUMS.txt";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseAssetInfo {
    pub name: String,
    pub download_url: String,
    pub size: u64,
}

impl From<&GitHubAsset> for ReleaseAssetInfo {
    fn from(asset: &GitHubAsset) -> Self {
        Self {
            name: asset.name.clone(),
            download_url: asset.browser_download_url.clone(),
            size: asset.size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequiredAssets {
    pub server: ReleaseAssetInfo,
    pub runtime: ReleaseAssetInfo,
    pub updater: ReleaseAssetInfo,
    pub checksums: ReleaseAssetInfo,
    pub msi: Option<ReleaseAssetInfo>,
}

pub fn select_required_assets(assets: &[GitHubAsset]) -> Result<RequiredAssets, UpdateError> {
    let server = find_asset(assets, SERVER_WINDOWS_ASSET_NAME)?;
    let runtime = find_asset(assets, STREMIO_RUNTIME_WINDOWS_ASSET_NAME)?;
    let updater = find_asset(assets, UPDATER_WINDOWS_ASSET_NAME)?;
    let checksums = find_asset(assets, CHECKSUMS_ASSET_NAME)?;
    let msi = assets
        .iter()
        .find(|asset| asset.name.to_ascii_lowercase().ends_with(".msi"))
        .map(ReleaseAssetInfo::from);

    Ok(RequiredAssets {
        server,
        runtime,
        updater,
        checksums,
        msi,
    })
}

fn find_asset(assets: &[GitHubAsset], name: &str) -> Result<ReleaseAssetInfo, UpdateError> {
    assets
        .iter()
        .find(|asset| asset.name == name)
        .map(ReleaseAssetInfo::from)
        .ok_or_else(|| {
            UpdateError::invalid_release(format!("release is missing required asset {name}"))
        })
}

pub fn parse_sha256sums(text: &str) -> HashMap<String, String> {
    let mut checksums = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((left, right)) = line.split_once("  ") {
            let hash = left.trim();
            let name = right.trim().trim_start_matches('*');
            if is_sha256(hash) && !name.is_empty() {
                checksums.insert(name.to_string(), hash.to_ascii_lowercase());
                continue;
            }
        }

        if let Some((left, hash)) = line.split_once('=') {
            let left = left.trim();
            let hash = hash.trim();
            if left.starts_with("SHA256(") && left.ends_with(')') && is_sha256(hash) {
                let name = &left["SHA256(".len()..left.len() - 1];
                checksums.insert(name.to_string(), hash.to_ascii_lowercase());
            }
        }
    }
    checksums
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub async fn fetch_checksum_text(
    client: &reqwest::Client, asset: &ReleaseAssetInfo,
) -> Result<String, UpdateError> {
    let text = client
        .get(&asset.download_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(text)
}

pub async fn download_asset(
    client: &reqwest::Client, asset: &ReleaseAssetInfo, dest: &Path,
) -> Result<(), UpdateError> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let response = client
        .get(&asset.download_url)
        .send()
        .await?
        .error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut file = tokio::fs::File::create(dest).await?;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(())
}

pub async fn sha256_file(path: &Path) -> Result<String, UpdateError> {
    let data = tokio::fs::read(path).await?;
    let digest = Sha256::digest(&data);
    Ok(hex::encode(digest))
}

pub async fn verify_file_checksum(
    asset: &ReleaseAssetInfo, path: &Path, checksums: &HashMap<String, String>,
) -> Result<String, UpdateError> {
    let expected = checksums.get(&asset.name).ok_or_else(|| {
        UpdateError::new(
            UpdateErrorKind::InvalidRelease,
            format!("checksum file does not contain {}", asset.name),
        )
    })?;
    let actual = sha256_file(path).await?;
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(UpdateError::new(
            UpdateErrorKind::InvalidRelease,
            format!(
                "checksum mismatch for {}: expected {}, got {}",
                asset.name, expected, actual
            ),
        ));
    }
    Ok(actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gh_asset(name: &str) -> GitHubAsset {
        GitHubAsset {
            name: name.to_string(),
            browser_download_url: format!("https://example.test/{name}"),
            size: 1,
        }
    }

    #[test]
    fn asset_selector_requires_all_windows_assets() {
        let assets = vec![
            gh_asset(SERVER_WINDOWS_ASSET_NAME),
            gh_asset(STREMIO_RUNTIME_WINDOWS_ASSET_NAME),
            gh_asset(CHECKSUMS_ASSET_NAME),
        ];

        let err = select_required_assets(&assets).unwrap_err();
        assert!(err.to_string().contains(UPDATER_WINDOWS_ASSET_NAME));
    }

    #[test]
    fn parses_sha256sum_file() {
        let hash = "a".repeat(64);
        let text = format!("{hash}  {SERVER_WINDOWS_ASSET_NAME}\n");
        let parsed = parse_sha256sums(&text);
        assert_eq!(
            parsed.get(SERVER_WINDOWS_ASSET_NAME),
            Some(&hash.to_ascii_lowercase())
        );
    }

    #[tokio::test]
    async fn hash_mismatch_is_rejected() {
        let root =
            std::env::temp_dir().join(format!("stream-server-assets-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join(SERVER_WINDOWS_ASSET_NAME);
        std::fs::write(&path, b"not-the-release-asset").unwrap();

        let asset = ReleaseAssetInfo {
            name: SERVER_WINDOWS_ASSET_NAME.to_string(),
            download_url: "https://example.test/server.exe".to_string(),
            size: 1,
        };
        let mut checksums = HashMap::new();
        checksums.insert(SERVER_WINDOWS_ASSET_NAME.to_string(), "b".repeat(64));

        let err = verify_file_checksum(&asset, &path, &checksums)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
        let _ = std::fs::remove_dir_all(&root);
    }
}
