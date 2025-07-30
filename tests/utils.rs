use pretty_assertions::assert_eq;
use std::{
    env, fs,
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
    process::Command,
};

pub const TMPDIR: &str = "tmp_tests";
pub const TMPDIR_SOURCE_BASEDIR: &str = "tmp_tests/resources/source";
pub const TMPDIR_TARGET: &str = "tmp_tests/target";
pub const TMPDIR_COMPARE: &str = "tmp_tests/resources/compare";
pub const TMPDIR_RESOURCELISTS: &str = "tmp_tests/resources/resourcelists";
pub const TEST_RESOURCE_DIR: &str = "tests/resources";

fn get_target_dir() -> PathBuf {
    let bin = env::current_exe().expect("exe path");
    let mut target_dir = PathBuf::from(bin.parent().expect("bin parent"));
    target_dir.pop();
    target_dir
}

pub fn migration_tool_path() -> String {
    let mut target_dir = get_target_dir();
    target_dir.push("proxmox-rrd-migration-tool");
    target_dir.to_str().unwrap().to_string()
}

/// Prepare the directory with the source files on which the tests are performed
pub fn test_prepare() {
    let tmpdir = Path::new(TMPDIR);

    println!("Setting up test tmp dir");
    if tmpdir.exists() {
        fs::remove_dir_all(tmpdir).expect("remove tmpdir");
    }
    fs::create_dir(tmpdir).expect("create tmpdir");
    fs::create_dir_all(TMPDIR_TARGET).expect("created tmp target dir");

    Command::new("cp")
        .args(["-ra", TEST_RESOURCE_DIR, TMPDIR])
        .output()
        .expect("copy test resource files");
}

/// Loop over directories to compare results
///
/// type:               type of test, node, guest, storage
/// target_path:        path to the dir where the target RRD files are
/// comp_subdir_prefix: subdir prefix where the target files are expetect to be per type
pub fn compare_results(migrationtype: &str, target_path: &PathBuf, comp_subdir_prefix: &str) {
    fs::read_dir(&target_path)
        .expect(format!("could not read target {migrationtype} dir").as_str())
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| f.is_file())
        .for_each(|file| {
            let path = file.as_path();

            let expected_path: PathBuf = [
                TMPDIR_COMPARE,
                format!(
                    "{}_{}",
                    comp_subdir_prefix,
                    file.file_name().unwrap().to_string_lossy()
                )
                .as_str(),
            ]
            .iter()
            .collect();
            let expected = fs::read_to_string(expected_path).expect("read compare file");
            let testcase = String::from_utf8(
                Command::new("rrdtool")
                    .args(["info", path.to_str().unwrap()])
                    .output()
                    .expect("execute rrdtool info")
                    .stdout,
            )
            .expect("rrdtool into to string");
            compare_rrdinfo_output(testcase, expected);
        });
}

/// Compares the output of rrdinfo with the expected output.
pub fn compare_rrdinfo_output(testcase: String, expected: String) {
    let expected_lines: Vec<String> = expected.lines().map(|l| String::from(l)).collect();
    let testcase_lines: Vec<String> = testcase.lines().map(|l| String::from(l)).collect();
    assert_eq!(
        expected_lines.len(),
        testcase_lines.len(),
        "expected: {}, testcase: {}",
        expected_lines.len(),
        testcase_lines.len()
    );
    for (expected, command) in expected_lines.iter().zip(testcase_lines.iter()) {
        if expected.contains("cur_row") || expected.contains("last_update") {
            // these lines can still have different values regarding timing and ptr positions
            continue;
        }
        assert_eq!(expected, command);
    }
}

/// Reads file and resturns it as a string, except for the last line
pub fn drop_last_line(content: Vec<u8>) -> String {
    let mut out: Vec<String> = Vec::new();
    let c = Cursor::new(content);
    let mut lines = c.lines();
    while let Some(line) = lines.next() {
        let line = line.expect("output line");
        out.push(line);
    }
    let _last_line = out.pop();
    let mut output = out.join("\n");
    output.push_str("\n");
    output
}
