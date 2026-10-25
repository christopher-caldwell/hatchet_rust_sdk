use super::*;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const RELEASES: &str = "https://github.com/hatchet-dev/hatchet-embedded/releases";
const MAX_BINARY: u64 = 512 * 1024 * 1024;
fn config(message: impl Into<String>) -> Error {
    Error::Config(message.into())
}
fn asset() -> Result<String> {
    let os=match std::env::consts::OS {"macos"=>"darwin","linux"=>"linux",_=>return Err(Error::UnsupportedOperation("embedded release downloads support macOS/Linux only; use a supplied native binary otherwise".into()))};
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => {
            return Err(Error::UnsupportedOperation(
                "embedded release downloads support amd64/arm64 only".into(),
            ));
        }
    };
    Ok(format!("hatchet-embedded-sidecar_{os}_{arch}"))
}
fn tag(value: &str) -> Result<String> {
    let version = value.strip_prefix('v').unwrap_or(value);
    semver::Version::parse(version)
        .map_err(|_| config("embedded version must be latest or a semantic release tag"))?;
    Ok(format!("v{version}"))
}
fn checksum(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(config(
            "embedded checksum must contain 64 SHA-256 hex digits",
        ));
    }
    Ok(value.to_ascii_lowercase())
}
async fn digest(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| config(format!("cannot open embedded binary: {e}")))?;
    if file
        .metadata()
        .await
        .map_err(|e| config(e.to_string()))?
        .len()
        > MAX_BINARY
    {
        return Err(config("embedded binary exceeds size limit"));
    }
    let mut total = 0_u64;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|e| config(format!("cannot read embedded binary: {e}")))?;
        if count == 0 {
            break;
        }
        total = total.saturating_add(count as u64);
        if total > MAX_BINARY {
            return Err(config("embedded binary exceeds size limit"));
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
async fn limited(response: reqwest::Response, maximum: usize) -> Result<Vec<u8>> {
    let mut response = response
        .error_for_status()
        .map_err(|e| config(format!("embedded release request failed: {e}")))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| config(format!("embedded release read failed: {e}")))?
    {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(config("embedded release metadata exceeds size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn version(requested: &str, releases: &str) -> Result<String> {
    if requested != "latest" {
        return tag(requested);
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| config(e.to_string()))?;
    let response = client
        .get(format!("{releases}/latest"))
        .send()
        .await
        .map_err(|e| config(format!("cannot resolve latest embedded release: {e}")))?;
    if !response.status().is_redirection() {
        return Err(config(
            "embedded latest-release endpoint did not redirect to a tag",
        ));
    }
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| config("embedded latest release omitted its location"))?;
    let base =
        reqwest::Url::parse(&format!("{releases}/latest")).map_err(|e| config(e.to_string()))?;
    let target = base.join(location).map_err(|e| config(e.to_string()))?;
    let prefix = format!("{}/tag/", releases.trim_end_matches('/'));
    let requested = target
        .as_str()
        .strip_prefix(&prefix)
        .ok_or_else(|| config("embedded latest release redirected outside its release tags"))?;
    tag(requested)
}
fn save_checksum(path: &Path, expected: &str) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(
        path.parent()
            .ok_or_else(|| config("embedded cache has no parent"))?,
    )
    .map_err(|e| config(e.to_string()))?;
    writeln!(temp, "{expected}").map_err(|e| config(e.to_string()))?;
    temp.as_file()
        .sync_all()
        .map_err(|e| config(e.to_string()))?;
    temp.persist(path)
        .map_err(|e| config(e.error.to_string()))?;
    Ok(())
}
async fn expected(
    client: &reqwest::Client,
    releases: &str,
    tag: &str,
    asset: &str,
    directory: &Path,
) -> Result<String> {
    let fetched = async {
        let response = client
            .get(format!("{releases}/download/{tag}/checksums.txt"))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| config(format!("cannot fetch embedded checksums: {e}")))?;
        let bytes = limited(response, 64 * 1024).await?;
        let text =
            std::str::from_utf8(&bytes).map_err(|_| config("embedded checksums are not UTF-8"))?;
        let mut found = None;
        for line in text.lines() {
            let parts = line.split_whitespace().collect::<Vec<_>>();
            if parts.len() == 2 && parts[1] == asset {
                if found.is_some() {
                    return Err(config("embedded checksums contain duplicate asset entries"));
                }
                found = Some(checksum(parts[0])?);
            }
        }
        found.ok_or_else(|| config("embedded checksums omit this platform asset"))
    }
    .await;
    match fetched {
        Ok(value) => Ok(value),
        Err(error) => {
            // Offline warm starts only trust a digest saved after verification.
            let checksum_path = directory.join(format!("{asset}.sha256"));
            if let Ok(file) = tokio::fs::File::open(checksum_path).await {
                let mut cached = String::new();
                if file.take(129).read_to_string(&mut cached).await.is_ok() {
                    let digest = checksum(&cached)?;
                    if tokio::fs::try_exists(directory.join(&digest).join(asset))
                        .await
                        .unwrap_or(false)
                    {
                        return Ok(digest);
                    }
                }
            }
            Err(error)
        }
    }
}
async fn ensure(
    options: &EmbeddedOptions,
    client: &reqwest::Client,
    releases: &str,
    requested: &str,
) -> Result<PathBuf> {
    let tag = version(requested, releases).await?;
    let asset = asset()?;
    let cache = match &options.cache_dir {
        Some(dir) => dir.clone(),
        None => PathBuf::from(
            std::env::var_os("HOME")
                .ok_or_else(|| config("set embedded cache_dir when HOME is unavailable"))?,
        )
        .join(".hatchet/embedded"),
    };
    let dir = cache.join(&tag);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| config(format!("cannot create embedded cache: {e}")))?;
    let expected = match &options.checksum {
        Some(value) => checksum(value)?,
        None => expected(client, releases, &tag, &asset, &dir).await?,
    };
    // Paths are immutable with respect to SDK installs of *different* bytes.
    // A concurrently updated release cannot replace a pinned caller's verified
    // executable between hashing it and spawning it.
    let content_dir = dir.join(&expected);
    tokio::fs::create_dir_all(&content_dir)
        .await
        .map_err(|e| config(e.to_string()))?;
    let path = content_dir.join(&asset);
    let release_checksum = dir.join(format!("{asset}.sha256"));
    if digest(&path).await.is_ok_and(|actual| actual == expected) {
        if options.checksum.is_none() {
            save_checksum(&release_checksum, &expected)?;
        }
        return tokio::fs::canonicalize(path)
            .await
            .map_err(|e| config(e.to_string()));
    }
    eprintln!("hatchet embedded: downloading sidecar {tag}");
    let response = client
        .get(format!("{releases}/download/{tag}/{asset}"))
        .timeout(Duration::from_secs(300))
        .send()
        .await
        .map_err(|e| config(format!("cannot download embedded sidecar: {e}")))?;
    let mut response = response
        .error_for_status()
        .map_err(|e| config(format!("embedded sidecar download failed: {e}")))?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BINARY)
    {
        return Err(config("embedded sidecar exceeds download size limit"));
    }
    let temporary =
        tempfile::NamedTempFile::new_in(&content_dir).map_err(|e| config(e.to_string()))?;
    let mut file =
        tokio::fs::File::from_std(temporary.reopen().map_err(|e| config(e.to_string()))?);
    let mut hash = Sha256::new();
    let mut count = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| config(format!("embedded sidecar download failed: {e}")))?
    {
        count = count.saturating_add(chunk.len() as u64);
        if count > MAX_BINARY {
            return Err(config("embedded sidecar exceeds download size limit"));
        }
        hash.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| config(e.to_string()))?;
    }
    file.flush().await.map_err(|e| config(e.to_string()))?;
    file.sync_all().await.map_err(|e| config(e.to_string()))?;
    drop(file);
    if format!("{:x}", hash.finalize()) != expected {
        return Err(config("embedded sidecar checksum mismatch"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o755))
            .map_err(|e| config(e.to_string()))?;
    }
    temporary
        .persist(&path)
        .map_err(|e| config(e.error.to_string()))?;
    if options.checksum.is_none() {
        save_checksum(&release_checksum, &expected)?;
    }
    tokio::fs::canonicalize(path)
        .await
        .map_err(|e| config(e.to_string()))
}
pub(super) async fn resolve(options: &EmbeddedOptions) -> Result<PathBuf> {
    if let Some(path) = options
        .binary_path
        .clone()
        .or_else(|| std::env::var_os("HATCHET_CLIENT_EMBEDDED_BINARY_PATH").map(PathBuf::from))
    {
        let path = tokio::fs::canonicalize(path)
            .await
            .map_err(|e| config(format!("cannot locate supplied embedded binary: {e}")))?;
        if !tokio::fs::metadata(&path)
            .await
            .map_err(|e| config(e.to_string()))?
            .is_file()
        {
            return Err(config("supplied embedded binary must be a file"));
        }
        if let Some(expected) = &options.checksum {
            if digest(&path).await? != checksum(expected)? {
                return Err(config("supplied embedded binary checksum mismatch"));
            }
        }
        return Ok(path);
    }
    let requested = options
        .version
        .clone()
        .or_else(|| std::env::var("HATCHET_CLIENT_EMBEDDED_VERSION").ok())
        .unwrap_or_else(|| "latest".into());
    let client = reqwest::Client::builder()
        .https_only(true)
        .build()
        .map_err(|e| config(e.to_string()))?;
    ensure(options, &client, RELEASES, &requested).await
}

#[cfg(all(
    test,
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct Mirror {
        url: String,
        hits: Arc<tokio::sync::Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Mirror {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn mirror(binary: Vec<u8>, expected: String) -> Mirror {
        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let url = format!("http://{}/releases", listener.local_addr().unwrap());
        let hits = Arc::new(tokio::sync::Mutex::new(vec![]));
        let task = tokio::spawn({
            let base = url.clone();
            let hits = Arc::clone(&hits);
            async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let binary = binary.clone();
                    let expected = expected.clone();
                    let base = base.clone();
                    let hits = Arc::clone(&hits);
                    tokio::spawn(async move {
                        let mut bytes = Vec::new();
                        while !bytes.windows(4).any(|b| b == b"\r\n\r\n") {
                            let mut buffer = [0; 4096];
                            let n = socket.read(&mut buffer).await.unwrap();
                            if n == 0 {
                                return;
                            }
                            bytes.extend_from_slice(&buffer[..n]);
                        }
                        let request = String::from_utf8(bytes).unwrap();
                        let path = request.split_whitespace().nth(1).unwrap().to_owned();
                        hits.lock().await.push(path.clone());
                        let (status, extra, body) = if path.ends_with("/latest") {
                            (
                                302,
                                format!("Location: {base}/tag/v0.110.5\r\n"),
                                Vec::new(),
                            )
                        } else if path.ends_with("/checksums.txt") {
                            (
                                200,
                                String::new(),
                                format!("{expected}  {}\n", asset().unwrap()).into_bytes(),
                            )
                        } else {
                            (200, String::new(), binary)
                        };
                        socket.write_all(format!("HTTP/1.1 {status} fixture\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                        socket.write_all(&body).await.unwrap();
                    });
                }
            }
        });
        Mirror { url, hits, task }
    }
    #[tokio::test]
    async fn cache_is_verified_repaired_and_reused_offline_with_a_pinned_release() {
        let dir = tempfile::tempdir().unwrap();
        let binary = b"verified binary".to_vec();
        let sha = format!("{:x}", Sha256::digest(&binary));
        let server = mirror(binary.clone(), sha.clone()).await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let options = EmbeddedOptions {
            cache_dir: Some(dir.path().into()),
            ..Default::default()
        };
        let path = ensure(&options, &client, &server.url, "latest")
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), binary);
        assert_eq!(server.hits.lock().await.len(), 3);
        tokio::fs::write(&path, b"tampered").await.unwrap();
        let repaired = ensure(&options, &client, &server.url, "v0.110.5")
            .await
            .unwrap();
        assert_eq!(repaired, path);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), binary);
        let address = server.url.clone();
        drop(server);
        let offline = ensure(&options, &client, &address, "v0.110.5")
            .await
            .unwrap();
        assert_eq!(offline, path);
        let pinned = EmbeddedOptions {
            checksum: Some(sha.to_uppercase()),
            ..options.clone()
        };
        assert_eq!(
            ensure(&pinned, &client, &address, "v0.110.5")
                .await
                .unwrap(),
            path
        );
        assert!(
            ensure(&options, &client, &address, "../../escape")
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn wrong_download_digest_never_installs_and_concurrent_downloads_do_not_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let options = EmbeddedOptions {
            cache_dir: Some(dir.path().into()),
            ..Default::default()
        };
        let server = mirror(b"wrong bytes".to_vec(), "00".repeat(32)).await;
        assert!(
            ensure(&options, &client, &server.url, "v0.110.5")
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("v0.110.5").join("00".repeat(32)))
                .unwrap()
                .count(),
            0
        );
        drop(server);
        let valid_dir = tempfile::tempdir().unwrap();
        let options = EmbeddedOptions {
            cache_dir: Some(valid_dir.path().into()),
            ..options
        };
        let binary = b"correct binary".to_vec();
        let sha = format!("{:x}", Sha256::digest(&binary));
        let server = mirror(binary.clone(), sha).await;
        let (a, b) = tokio::join!(
            ensure(&options, &client, &server.url, "v0.110.5"),
            ensure(&options, &client, &server.url, "v0.110.5")
        );
        let installed = a.unwrap();
        assert_eq!(installed, b.unwrap());
        assert_eq!(tokio::fs::read(installed).await.unwrap(), binary);
        assert_eq!(
            std::fs::read_dir(valid_dir.path().join("v0.110.5"))
                .unwrap()
                .count(),
            2
        );
    }
    #[tokio::test]
    async fn differing_same_tag_digest_policies_never_replace_a_returned_pinned_executable() {
        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let a = b"pinned executable A".to_vec();
        let b = b"replacement release B".to_vec();
        let sha_a = format!("{:x}", Sha256::digest(&a));
        let sha_b = format!("{:x}", Sha256::digest(&b));
        let release_a = mirror(a.clone(), sha_a.clone()).await;
        let release_b = mirror(b.clone(), sha_b.clone()).await;
        let pinned = EmbeddedOptions {
            cache_dir: Some(dir.path().into()),
            checksum: Some(sha_a.clone()),
            ..Default::default()
        };
        let pinned_path = ensure(&pinned, &client, &release_a.url, "v0.110.5")
            .await
            .unwrap();
        // Interleave a different release policy after the pinned caller returned
        // its executable path, but before that caller would spawn the process.
        let unpinned = EmbeddedOptions {
            checksum: None,
            ..pinned.clone()
        };
        let replacement_path = ensure(&unpinned, &client, &release_b.url, "v0.110.5")
            .await
            .unwrap();
        assert_ne!(pinned_path, replacement_path);
        assert_eq!(tokio::fs::read(&pinned_path).await.unwrap(), a);
        assert_eq!(tokio::fs::read(&replacement_path).await.unwrap(), b);
        assert_eq!(digest(&pinned_path).await.unwrap(), sha_a);
        assert_eq!(
            ensure(&pinned, &client, &release_b.url, "v0.110.5")
                .await
                .unwrap(),
            pinned_path
        );
        let marker = dir
            .path()
            .join("v0.110.5")
            .join(format!("{}.sha256", asset().unwrap()));
        assert_eq!(
            tokio::fs::read_to_string(marker).await.unwrap().trim(),
            sha_b,
            "a pin must not replace the release's offline reference"
        );
    }
}
