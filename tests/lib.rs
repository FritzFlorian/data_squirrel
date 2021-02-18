extern crate assert_cmd;
extern crate tempfile;

#[cfg(test)]
mod tests {
    use assert_cmd::Command;
    use std::io::Write;
    use tempfile::TempDir;

    fn main_cmd() -> Command {
        Command::cargo_bin("main").unwrap()
    }

    fn create_file(dir: &TempDir, path: &str, content: &str) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dir.path().join(path))
            .unwrap();

        file.write_all(content.as_bytes()).unwrap();
    }

    fn assert_file(dir: &TempDir, path: &str, target_content: &str) {
        let content = std::fs::read_to_string(dir.path().join(path)).unwrap();
        assert_eq!(content, target_content);
    }

    fn cmd_success(dir: &TempDir, cmd: &str, args: Vec<&str>) {
        main_cmd()
            .arg(dir.path())
            .arg(cmd)
            .args(args)
            .assert()
            .success();
    }

    #[test]
    fn basic_two_folder_sync() {
        let dir_1 = tempfile::tempdir().unwrap();
        let dir_2 = tempfile::tempdir().unwrap();

        create_file(&dir_1, "file-1", "content 1");
        create_file(&dir_2, "file-2", "content 2");

        cmd_success(&dir_1, "create", vec!["--name='XYZ'"]);
        cmd_success(&dir_2, "create", vec!["--name='XYZ'"]);

        cmd_success(&dir_1, "scan", vec![]);
        cmd_success(&dir_2, "scan", vec![]);

        cmd_success(&dir_1, "sync-from", vec![dir_2.path().to_str().unwrap()]);
        cmd_success(&dir_2, "sync-from", vec![dir_1.path().to_str().unwrap()]);

        assert_file(&dir_1, "file-1", "content 1");
        assert_file(&dir_1, "file-2", "content 2");
        assert_file(&dir_2, "file-1", "content 1");
        assert_file(&dir_2, "file-2", "content 2");
    }
}
