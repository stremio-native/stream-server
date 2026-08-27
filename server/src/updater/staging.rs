use super::{
    assets::{
        ReleaseAssetInfo, download_asset, fetch_checksum_text, parse_sha256sums,
        verify_file_checksum,
    },
    error::{UpdateError, UpdateErrorKind},
    state::UpdateCandidate,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StagedAssetPaths {
    pub server: PathBuf,
    pub runtime: PathBuf,
    pub updater: PathBuf,
    pub checksums: PathBuf,
    pub msi: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StagedUpdate {
    pub version: String,
    pub tag: String,
    pub release_url: String,
    pub dir: PathBuf,
    pub assets: StagedAssetPaths,
    pub hashes: HashMap<String, String>,
}

pub async fn stage_update(
    client: &reqwest::Client, staging_root: PathBuf, candidate: &UpdateCandidate,
) -> Result<StagedUpdate, UpdateError> {
    let dir = staging_root.join(format!("v{}", candidate.version));
    if dir.exists() {
        tokio::fs::remove_dir_all(&dir).await?;
    }
    tokio::fs::create_dir_all(&dir).await?;

    let checksum_text = fetch_checksum_text(client, &candidate.assets.checksums).await?;
    let checksums = parse_sha256sums(&checksum_text);
    if checksums.is_empty() {
        return Err(UpdateError::new(
            UpdateErrorKind::InvalidRelease,
            "SHA256SUMS.txt did not contain any usable checksums",
        ));
    }

    let checksum_path = dir.join(&candidate.assets.checksums.name);
    tokio::fs::write(&checksum_path, checksum_text).await?;

    let server = download_and_verify(client, &dir, &candidate.assets.server, &checksums).await?;
    let runtime = download_and_verify(client, &dir, &candidate.assets.runtime, &checksums).await?;
    let updater = download_and_verify(client, &dir, &candidate.assets.updater, &checksums).await?;
    let msi = if let Some(msi) = &candidate.assets.msi {
        Some(download_and_verify(client, &dir, msi, &checksums).await?)
    } else {
        None
    };

    let staged = StagedUpdate {
        version: candidate.version.clone(),
        tag: candidate.tag.clone(),
        release_url: candidate.release_url.clone(),
        dir: dir.clone(),
        assets: StagedAssetPaths {
            server,
            runtime,
            updater,
            checksums: checksum_path,
            msi,
        },
        hashes: checksums,
    };
    let manifest = serde_json::to_string_pretty(&staged)
        .map_err(|err| UpdateError::new(UpdateErrorKind::Unexpected, err.to_string()))?;
    tokio::fs::write(dir.join("staged-update.json"), manifest).await?;

    Ok(staged)
}

async fn download_and_verify(
    client: &reqwest::Client, dir: &std::path::Path, asset: &ReleaseAssetInfo,
    checksums: &HashMap<String, String>,
) -> Result<PathBuf, UpdateError> {
    let dest = dir.join(&asset.name);
    download_asset(client, asset, &dest).await?;
    verify_file_checksum(asset, &dest, checksums).await?;
    Ok(dest)
}
