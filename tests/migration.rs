use anyhow::Error;
use pretty_assertions::assert_eq;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

mod utils;

use utils::{TMPDIR, TMPDIR_RESOURCELISTS, TMPDIR_SOURCE_BASEDIR, TMPDIR_TARGET};

const TARGET_SUBDIR_NODE: &str = "pve-node-9.0";
const TARGET_SUBDIR_GUEST: &str = "pve-vm-9.0";
const TARGET_SUBDIR_STORAGE: &str = "pve-storage-9.0";

#[test]
fn migration() {
    utils::test_prepare();

    let target_dir_guests: PathBuf = [TMPDIR_TARGET, TARGET_SUBDIR_GUEST].iter().collect();
    let target_dir_nodes: PathBuf = [TMPDIR_TARGET, TARGET_SUBDIR_NODE].iter().collect();
    let target_dir_storage: PathBuf = [TMPDIR_TARGET, TARGET_SUBDIR_STORAGE].iter().collect();

    // first test, compare resulting rrd files
    Command::new("faketime")
        .arg("2025-08-01 00:00:00")
        .arg(utils::migration_tool_path())
        .arg("--migrate")
        .arg("--source")
        .arg(TMPDIR_SOURCE_BASEDIR)
        .arg("--target")
        .arg(TMPDIR_TARGET)
        .arg("--resources")
        .arg(TMPDIR_RESOURCELISTS)
        .output()
        .expect("failed to execute proxmox-rrd-migration-tool");

    // assert target files as we expect them
    assert!(Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_NODE}/testnode").as_str()).exists());
    assert!(Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_GUEST}/100").as_str()).exists());
    assert!(!Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_GUEST}/400").as_str()).exists());
    assert!(!Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_GUEST}/400.old").as_str()).exists());
    assert!(!Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_GUEST}/500.old").as_str()).exists());
    assert!(
        Path::new(format!("{TMPDIR_TARGET}/{TARGET_SUBDIR_STORAGE}/testnode/iso").as_str())
            .exists()
    );
    assert!(Path::new(format!("{TMPDIR_SOURCE_BASEDIR}/pve2-vm/100.old").as_str()).exists());
    assert!(Path::new(format!("{TMPDIR_SOURCE_BASEDIR}/pve2-vm/400.old").as_str()).exists());

    // compare
    utils::compare_results("node", &target_dir_nodes, &TARGET_SUBDIR_NODE);

    utils::compare_results("guest", &target_dir_guests, &TARGET_SUBDIR_GUEST);

    // storage has another layer of directories per node over which we need to iterate
    fs::read_dir(&target_dir_storage)
        .expect("could not read target storage dir")
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| f.is_dir())
        .try_for_each(|node| {
            let mut source_storage_subdir = target_dir_storage.clone();
            source_storage_subdir.push(node.file_name().unwrap());

            let mut target_storage_subdir = target_dir_storage.clone();
            target_storage_subdir.push(node.file_name().unwrap());

            utils::compare_results(
                "storage",
                &source_storage_subdir,
                format!(
                    "{TARGET_SUBDIR_STORAGE}_{}",
                    node.file_name().unwrap().to_string_lossy()
                )
                .as_str(),
            );
            Ok::<(), Error>(())
        })
        .expect("Error running storage test");
}
#[test]
fn migration_second_empty_run() {
    utils::test_prepare();

    // run initial migration
    Command::new("faketime")
        .arg("2025-08-01 00:00:00")
        .arg(utils::migration_tool_path())
        .arg("--migrate")
        .arg("--source")
        .arg(TMPDIR_SOURCE_BASEDIR)
        .arg("--target")
        .arg(TMPDIR_TARGET)
        .arg("--resources")
        .arg(TMPDIR_RESOURCELISTS)
        .output()
        .expect("failed to execute proxmox-rrd-migration-tool");

    // check if output skips all currently existing files
    let output = Command::new("faketime")
        .arg("2025-08-01 00:00:00")
        .arg(utils::migration_tool_path())
        .arg("--threads")
        .arg("2")
        .arg("--migrate")
        .arg("--source")
        .arg(TMPDIR_SOURCE_BASEDIR)
        .arg("--target")
        .arg(TMPDIR_TARGET)
        .arg("--resources")
        .arg(TMPDIR_RESOURCELISTS)
        .output()
        .expect("failed to execute proxmox-rrd-migration-tool");
    let expected_path: PathBuf = [TMPDIR, "resources", "compare", "second_empty_run"]
        .iter()
        .collect();

    let expected =
        fs::read_to_string(expected_path).expect("could not read compare file for skip all");

    assert_eq!(
        expected,
        String::from_utf8(output.stdout).expect("could not parse output")
    );
}

#[test]
fn migration_second_run_with_missed_files() {
    utils::test_prepare();

    // run initial migration
    Command::new("faketime")
        .arg("2025-08-01 00:00:00")
        .arg(utils::migration_tool_path())
        .arg("--migrate")
        .arg("--source")
        .arg(TMPDIR_SOURCE_BASEDIR)
        .arg("--target")
        .arg(TMPDIR_TARGET)
        .arg("--resources")
        .arg(TMPDIR_RESOURCELISTS)
        .output()
        .expect("failed to execute proxmox-rrd-migration-tool");

    let src_vm = format!("{TMPDIR_SOURCE_BASEDIR}/pve2-vm/100.old");
    let target_vm = format!("{TMPDIR_SOURCE_BASEDIR}/pve2-vm/101");

    Command::new("cp")
        .args([src_vm, target_vm])
        .output()
        .expect("copy 101 rrd file");

    // check if output skips all currently existing files
    let output = Command::new("faketime")
        .arg("2025-08-01 00:00:00")
        .arg(utils::migration_tool_path())
        .arg("--threads")
        .arg("2")
        .arg("--migrate")
        .arg("--source")
        .arg(TMPDIR_SOURCE_BASEDIR)
        .arg("--target")
        .arg(TMPDIR_TARGET)
        .arg("--resources")
        .arg(TMPDIR_RESOURCELISTS)
        .output()
        .expect("failed to execute proxmox-rrd-migration-tool");

    let expected_path: PathBuf = [TMPDIR, "resources", "compare", "second_run_with_missed"]
        .iter()
        .collect();

    let expected = fs::read_to_string(expected_path.as_path())
        .expect("could not read compare file for skip all");

    // drop last line from output as it contains timing information which can change between tests
    let output = utils::drop_last_line(output.stdout);

    println!("OUTPUT:\n{}", output);
    println!("EXPECTED:\n{}", expected);

    assert_eq!(expected, output);
}
