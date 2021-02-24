extern crate assert_cmd;
extern crate predicates;
extern crate tempfile;

#[cfg(test)]
mod tests {
    use assert_cmd::Command;
    use predicates::prelude::*;
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

    fn dir_content(dir: &TempDir, path: &str, expected_items: Vec<&str>) {
        let content: Vec<_> = std::fs::read_dir(dir.path().join(path))
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            content.len(),
            expected_items.len(),
            "Directory must contain the expected number of items!"
        );
        for expected_item in expected_items {
            assert!(content
                .iter()
                .any(|item| item.as_ref().unwrap().file_name().to_str().unwrap() == expected_item))
        }
    }

    fn cmd_success(dir: &TempDir, cmd: &str, args: Vec<&str>) {
        let mut main_cmd = main_cmd();
        main_cmd
            .arg(dir.path())
            .arg(cmd)
            .args(args)
            .assert()
            .success();
    }
    fn cmd_should_print(dir: &TempDir, cmd: &str, args: Vec<&str>, expected: &str) {
        main_cmd()
            .arg(dir.path())
            .arg(cmd)
            .args(args)
            .assert()
            .stdout(predicate::function(|output: &str| {
                output.contains(&expected)
            }));
    }

    #[test]
    fn basic_two_folder_sync() {
        let dir_1 = tempfile::tempdir().unwrap();
        let dir_2 = tempfile::tempdir().unwrap();
        cmd_success(&dir_1, "create", vec!["--name='XYZ'"]);
        cmd_success(&dir_2, "create", vec!["--name='XYZ'"]);

        create_file(&dir_1, "file-1", "content 1");
        create_file(&dir_2, "file-2", "content 2");
        cmd_success(&dir_1, "scan", vec![]);
        cmd_success(&dir_2, "scan", vec![]);

        cmd_success(&dir_1, "sync-from", vec![dir_2.path().to_str().unwrap()]);
        cmd_success(&dir_2, "sync-from", vec![dir_1.path().to_str().unwrap()]);

        assert_file(&dir_1, "file-1", "content 1");
        assert_file(&dir_1, "file-2", "content 2");
        assert_file(&dir_2, "file-1", "content 1");
        assert_file(&dir_2, "file-2", "content 2");
    }

    #[test]
    fn basic_two_folder_conflict_resolution() {
        let dir_1 = tempfile::tempdir().unwrap();
        let dir_2 = tempfile::tempdir().unwrap();
        cmd_success(&dir_1, "create", vec!["--name='XYZ'"]);
        cmd_success(&dir_2, "create", vec!["--name='XYZ'"]);

        create_file(&dir_1, "file-1", "content 1");
        create_file(&dir_2, "file-1", "content 2");
        cmd_success(&dir_1, "scan", vec![]);
        cmd_success(&dir_2, "scan", vec![]);

        cmd_should_print(
            &dir_1,
            "sync-from",
            vec![dir_2.path().to_str().unwrap()],
            "Do not resolve the conflict",
        );

        cmd_success(
            &dir_1,
            "sync-from",
            vec![dir_2.path().to_str().unwrap(), "--choose-local"],
        );
        cmd_success(&dir_2, "sync-from", vec![dir_1.path().to_str().unwrap()]);

        assert_file(&dir_1, "file-1", "content 1");
        assert_file(&dir_2, "file-1", "content 1");
    }

    #[test]
    fn basic_ignore_rules() {
        let dir_1 = tempfile::tempdir().unwrap();
        let dir_2 = tempfile::tempdir().unwrap();
        cmd_success(&dir_1, "create", vec!["--name='XYZ'"]);
        cmd_success(&dir_2, "create", vec!["--name='XYZ'"]);

        create_file(&dir_1, "file-1-1", "content 1-1");
        create_file(&dir_1, "file-1-2", "content 1-2");
        create_file(&dir_2, "file-2-1", "content 2-1");
        create_file(&dir_2, "file-2-2", "content 2-2");
        cmd_success(&dir_1, "scan", vec![]);
        cmd_success(&dir_2, "scan", vec![]);

        cmd_success(
            &dir_1,
            "rules",
            vec!["--ignore-rule=**/file-2-1", "--ignore-rule=**/file-2-2"],
        );
        cmd_success(&dir_1, "rules", vec!["--print"]);

        cmd_success(&dir_1, "sync-from", vec![dir_2.path().to_str().unwrap()]);
        cmd_success(&dir_2, "sync-from", vec![dir_1.path().to_str().unwrap()]);

        dir_content(
            &dir_1,
            "",
            vec![".__data_squirrel__", "file-1-1", "file-1-2"],
        );
        dir_content(
            &dir_2,
            "",
            vec![
                ".__data_squirrel__",
                "file-2-1",
                "file-2-2",
                "file-1-1",
                "file-1-2",
            ],
        );
    }
}
