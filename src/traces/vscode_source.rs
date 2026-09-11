//! Trace source for VS Code's native chat session stores.

use super::source::{TraceSource, Unit};
use super::vscode;
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub struct VscodeSource {
    root: PathBuf,
    limit: Option<usize>,
    cwds: RefCell<HashMap<String, Option<String>>>,
}

impl VscodeSource {
    pub fn new(root: PathBuf, limit: Option<usize>) -> Self {
        Self {
            root: canonical(root),
            limit,
            cwds: RefCell::new(HashMap::new()),
        }
    }

    fn files(&self) -> Result<Vec<PathBuf>> {
        discover(&self.root)
    }

    fn signature(path: &Path) -> Option<String> {
        let base = stat_sig(path)?;
        let workspace = vscode::metadata_path(path)
            .and_then(|p| stat_sig(&p))
            .unwrap_or_default();
        Some(format!("{base}|workspace:{workspace}"))
    }
}

impl TraceSource for VscodeSource {
    fn describe(&self) -> String {
        format!("scanning VS Code chat sessions under {}", self.root.display())
    }

    fn units(&self) -> Result<Vec<Unit>> {
        let mut paths = self.files()?;
        paths.sort_by(|a, b| {
            let am = mtime(a);
            let bm = mtime(b);
            bm.cmp(&am).then_with(|| a.cmp(b))
        });
        if let Some(limit) = self.limit {
            paths.truncate(limit);
        }
        Ok(paths
            .into_iter()
            .map(|path| Unit {
                key: path.to_string_lossy().into_owned(),
                signature: Self::signature(&path),
                is_subagent: false,
            })
            .collect())
    }

    fn read(&self, unit: &Unit) -> Result<Vec<super::Turn>> {
        let path = Path::new(&unit.key);
        let before = Self::signature(path);
        let parsed = vscode::read_session(path)?;
        if before != Self::signature(path) {
            anyhow::bail!("VS Code chat session changed while reading {}", path.display());
        }
        self.cwds.borrow_mut().insert(unit.key.clone(), parsed.cwd);
        Ok(parsed.turns)
    }

    fn owns(&self, key: &str) -> bool {
        let key = Path::new(key);
        if key.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
            return false;
        }
        let normalized = key
            .canonicalize()
            .ok()
            .or_else(|| Some(key.parent()?.canonicalize().ok()?.join(key.file_name()?)))
            .unwrap_or_else(|| key.to_path_buf());
        owns_lexical(&self.root, &normalized)
    }

    fn unit_keys(&self) -> Result<Vec<String>> {
        Ok(self
            .files()?
            .into_iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect())
    }

    fn cwd(&self, unit: &Unit) -> Option<String> {
        self.cwds.borrow().get(&unit.key).cloned().flatten()
    }
}

/// Existing standard VS Code user-data roots for a host platform.
pub fn user_data_roots(
    home: &Path,
    platform: &str,
    appdata: Option<&Path>,
    xdg_config_home: Option<&Path>,
) -> Vec<PathBuf> {
    let names = ["Code", "Code - Insiders"];
    let base = match platform {
        "macos" | "darwin" => Some(home.join("Library/Application Support")),
        "windows" => appdata.map(Path::to_path_buf),
        "linux" => Some(
            xdg_config_home
                .map(Path::to_path_buf)
                .unwrap_or_else(|| home.join(".config")),
        ),
        _ => None,
    };
    let Some(base) = base else { return Vec::new() };
    names
        .iter()
        .map(|name| base.join(name))
        .filter(|p| p.is_dir())
        .collect()
}

fn mtime(path: &Path) -> std::time::SystemTime {
    std::fs::metadata(path).and_then(|m| m.modified()).unwrap_or(UNIX_EPOCH)
}

fn stat_sig(path: &Path) -> Option<String> {
    let md = std::fs::metadata(path).ok()?;
    let ns = md.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_nanos();
    Some(format!("{}:{ns}", md.len()))
}

fn is_session_file(path: &Path) -> bool {
    matches!(path.extension().and_then(|e| e.to_str()), Some("json") | Some("jsonl"))
}

fn add_session_dir(dir: &Path, out: &mut BTreeSet<PathBuf>) -> Result<()> {
    let Some(entries) = entries(dir)? else { return Ok(()) };
    let mut json = BTreeSet::new();
    let mut jsonl = BTreeSet::new();
    for entry in entries {
        let path = entry.path();
        if !path.is_file() || !is_session_file(&path) {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            jsonl.insert((stem, path));
        } else {
            json.insert((stem, path));
        }
    }
    for (_, path) in jsonl {
        out.insert(canonical(path));
    }
    for (stem, path) in json {
        if !jsonl_stem_exists(dir, &stem) {
            out.insert(canonical(path));
        }
    }
    Ok(())
}

fn jsonl_stem_exists(dir: &Path, stem: &str) -> bool {
    dir.join(format!("{stem}.jsonl")).is_file()
}

fn scan_workspace_storage(dir: &Path, out: &mut BTreeSet<PathBuf>) -> Result<()> {
    let Some(entries) = entries(dir)? else { return Ok(()) };
    for entry in entries {
        let p = entry.path();
        if p.is_dir() {
            add_session_dir(&p.join("chatSessions"), out)?;
        }
    }
    Ok(())
}

fn scan_profile_root(root: &Path, out: &mut BTreeSet<PathBuf>) -> Result<()> {
    add_session_dir(&root.join("emptyWindowChatSessions"), out)?;
    add_session_dir(&root.join("globalStorage/emptyWindowChatSessions"), out)?;
    scan_workspace_storage(&root.join("workspaceStorage"), out)?;
    let profiles = root.join("profiles");
    if let Some(entries) = entries(&profiles)? {
        for entry in entries {
            if entry.path().is_dir() {
                scan_profile_contents(&entry.path(), out)?;
            }
        }
    }
    Ok(())
}

fn scan_profile_contents(root: &Path, out: &mut BTreeSet<PathBuf>) -> Result<()> {
    add_session_dir(&root.join("emptyWindowChatSessions"), out)?;
    add_session_dir(&root.join("globalStorage/emptyWindowChatSessions"), out)?;
    scan_workspace_storage(&root.join("workspaceStorage"), out)
}

fn entries(dir: &Path) -> Result<Option<Vec<std::fs::DirEntry>>> {
    match std::fs::read_dir(dir) {
        Ok(iter) => Ok(Some(
            iter.collect::<std::io::Result<Vec<_>>>()
                .with_context(|| format!("reading {}", dir.display()))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", dir.display())),
    }
}

fn discover(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = BTreeSet::new();
    if root.is_file() {
        if is_session_file(root) {
            let preferred = root.with_extension("jsonl");
            out.insert(canonical(if preferred.is_file() {
                preferred
            } else {
                root.to_path_buf()
            }));
        }
    } else if root.is_dir() {
        let name = root.file_name().and_then(|s| s.to_str()).unwrap_or_default();
        match name {
            "chatSessions" | "emptyWindowChatSessions" => add_session_dir(root, &mut out)?,
            "workspaceStorage" => scan_workspace_storage(root, &mut out)?,
            "globalStorage" => add_session_dir(&root.join("emptyWindowChatSessions"), &mut out)?,
            "User" => scan_profile_root(root, &mut out)?,
            _ => {
                scan_profile_root(&root.join("User"), &mut out)?;
                scan_profile_root(&root.join("user-data"), &mut out)?;
                scan_profile_root(root, &mut out)?;
            }
        }
    }
    Ok(out.into_iter().collect())
}

fn owns_lexical(root: &Path, key: &Path) -> bool {
    if !key.is_absolute() || key.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
        return false;
    }
    if root.is_file() || is_session_file(root) {
        return key == root || (root.extension().is_some_and(|e| e == "json") && key == root.with_extension("jsonl"));
    }
    let Ok(rel) = key.strip_prefix(root) else { return false };
    let parts: Vec<_> = rel
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();
    if parts.is_empty() || !matches!(key.extension().and_then(|e| e.to_str()), Some("json" | "jsonl")) {
        return false;
    }
    let dirs = &parts[..parts.len() - 1];
    let root_name = root.file_name().and_then(|s| s.to_str()).unwrap_or_default();
    match root_name {
        "chatSessions" | "emptyWindowChatSessions" => dirs.is_empty(),
        "workspaceStorage" => dirs.len() == 2 && dirs[1] == "chatSessions",
        "globalStorage" => dirs == ["emptyWindowChatSessions"],
        "User" => recognized_user_dirs(dirs),
        _ => {
            recognized_user_dirs(dirs)
                || (dirs.first().is_some_and(|p| *p == "User" || *p == "user-data") && recognized_user_dirs(&dirs[1..]))
        }
    }
}

fn recognized_user_dirs(dirs: &[&str]) -> bool {
    let dirs = if dirs.first() == Some(&"profiles") && dirs.len() > 2 {
        &dirs[2..] // profiles/<profile-id>/...
    } else {
        dirs
    };
    dirs == ["emptyWindowChatSessions"]
        || dirs == ["globalStorage", "emptyWindowChatSessions"]
        || (dirs.len() == 3 && dirs[0] == "workspaceStorage" && dirs[2] == "chatSessions")
}

fn canonical(path: PathBuf) -> PathBuf {
    std::fs::canonicalize(&path).unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn explicit_file_and_prefer_jsonl() {
        let d = tempdir().unwrap();
        let dir = d.path().join("chatSessions");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.json"), "{}").unwrap();
        std::fs::write(dir.join("a.jsonl"), "{}").unwrap();
        let src = VscodeSource::new(dir.clone(), None);
        assert_eq!(src.unit_keys().unwrap().len(), 1);
        assert!(src.owns(dir.join("a.jsonl").to_str().unwrap()));
        assert!(!src.owns(d.path().join("a.json").to_str().unwrap()));
    }

    #[test]
    fn workspace_and_profile_scopes_exclude_neighbors() {
        let d = tempdir().unwrap();
        let ws = d.path().join("User/workspaceStorage/id/chatSessions");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("ok.json"), "{}").unwrap();
        std::fs::write(d.path().join("User/settings.json"), "{}").unwrap();
        assert_eq!(
            VscodeSource::new(d.path().join("User"), None)
                .unit_keys()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn roots_only_existing_standard_dirs() {
        let d = tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".config/Code")).unwrap();
        assert_eq!(user_data_roots(d.path(), "linux", None, None).len(), 1);
        assert!(user_data_roots(d.path(), "unknown", None, None).is_empty());
    }

    #[test]
    fn stat_signature_includes_nanoseconds() {
        let d = tempdir().unwrap();
        let p = d.path().join("x.json");
        std::fs::write(&p, "{} ").unwrap();
        let first = stat_sig(&p).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&p, "[] ").unwrap();
        assert_ne!(first, stat_sig(&p).unwrap());
    }

    #[test]
    fn trait_cwd_dispatches_to_vscode_parser() {
        let d = tempdir().unwrap();
        let dir = d.path().join("chatSessions");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a.json");
        std::fs::write(
            &p,
            r#"{"version":3,"sessionId":"s","requests":[],"workingDirectory":"file:///tmp/project"}"#,
        )
        .unwrap();
        let src = VscodeSource::new(dir, None);
        let unit = src.units().unwrap().pop().unwrap();
        TraceSource::read(&src, &unit).unwrap();
        assert_eq!(TraceSource::cwd(&src, &unit).as_deref(), Some("/tmp/project"));
    }
    #[test]
    fn platform_roots_and_environment_overrides() {
        let home = tempdir().unwrap();
        let config = tempdir().unwrap();
        for name in ["Code", "Code - Insiders"] {
            std::fs::create_dir_all(home.path().join("Library/Application Support").join(name)).unwrap();
            std::fs::create_dir_all(config.path().join(name)).unwrap();
        }
        assert_eq!(user_data_roots(home.path(), "macos", None, None).len(), 2);
        assert_eq!(
            user_data_roots(home.path(), "windows", Some(config.path()), None).len(),
            2
        );
        assert!(user_data_roots(home.path(), "windows", None, None).is_empty());
        assert_eq!(
            user_data_roots(home.path(), "linux", None, Some(config.path())).len(),
            2
        );
        assert!(user_data_roots(home.path(), "linux", None, None).is_empty());
    }

    #[test]
    fn profiles_limits_metadata_and_retired_keys() {
        let home = tempdir().unwrap();
        let root = home.path().join("user-data");
        let paths = [
            "User/workspaceStorage/project/chatSessions/workspace.jsonl",
            "User/globalStorage/emptyWindowChatSessions/default.json",
            "User/profiles/p1/globalStorage/emptyWindowChatSessions/profile.json",
        ];
        for path in paths {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "{}").unwrap();
        }
        for path in [
            "User/globalStorage/chatSessionTransfer/data.json",
            "logs/debug.jsonl",
            "User/profiles/p1/settings.json",
            "User/workspaceStorage/project/state.vscdb",
        ] {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "{}").unwrap();
        }
        let source = VscodeSource::new(root.clone(), Some(1));
        let keys = source.unit_keys().unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(source.units().unwrap().len(), 1);
        for key in &keys {
            assert!(source.owns(key));
        }
        let session = root.join(paths[0]);
        let before = VscodeSource::signature(&session);
        std::fs::write(
            session.parent().unwrap().parent().unwrap().join("workspace.json"),
            r#"{"folder":"file:///work/project"}"#,
        )
        .unwrap();
        assert_ne!(before, VscodeSource::signature(&session));
        // A removed source key remains owned so coverage can account for it. Memory deletion is
        // never requested by source discovery.
        let removed = &keys[0];
        std::fs::rename(removed, home.path().join("outside-store")).unwrap();
        assert!(source.owns(removed));
        assert_eq!(source.unit_keys().unwrap().len(), 2);
        assert!(!source.owns(root.join("User/settings.json").to_str().unwrap()));
    }

    #[test]
    fn preferred_jsonl_errors_do_not_fall_back_to_stale_json() {
        let dir = tempdir().unwrap();
        let json = dir.path().join("session.json");
        std::fs::write(&json, r#"{"version":3,"requests":[]}"#).unwrap();
        let jsonl = json.with_extension("jsonl");
        std::fs::write(&jsonl, "{broken").unwrap();
        let source = VscodeSource::new(json, None);
        let unit = source.units().unwrap().pop().unwrap();
        assert_eq!(Path::new(&unit.key), canonical(jsonl));
        assert!(source.read(&unit).is_err());
    }
}
