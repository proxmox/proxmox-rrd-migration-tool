use anyhow::{bail, Error, Result};
use std::{
    ffi::{CStr, CString, OsString},
    fs,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

use proxmox_rrd_migration_tool::{rrd_clear_error, rrd_create_r2, rrd_get_context, rrd_get_error};

use crate::parallel_handler::ParallelHandler;

pub mod parallel_handler;

const BASE_DIR: &str = "/var/lib/rrdcached/db";
const SOURCE_SUBDIR_NODE: &str = "pve2-node";
const SOURCE_SUBDIR_GUEST: &str = "pve2-vm";
const SOURCE_SUBDIR_STORAGE: &str = "pve2-storage";
const TARGET_SUBDIR_NODE: &str = "pve-node-9.0";
const TARGET_SUBDIR_GUEST: &str = "pve-vm-9.0";
const TARGET_SUBDIR_STORAGE: &str = "pve-storage-9.0";
const RESOURCE_BASE_DIR: &str = "/etc/pve";
const MAX_AUTO_THREADS: usize = 6;
const RRD_STEP_SIZE: usize = 60;

type File = (CString, OsString);

// RRAs are defined in the following way:
//
// RRA:CF:xff:step:rows
// CF: AVERAGE or MAX
// xff: 0.5
// steps: stepsize is defined on rrd file creation! example: with 60 seconds step size:
//	e.g. 1 => 60 sec, 30 => 1800 seconds or 30 min
// rows: how many aggregated rows are kept, as in how far back in time we store data
//
// how many seconds are aggregated per RRA: steps * stepsize * rows
// how many hours are aggregated per RRA: steps * stepsize * rows / 3600
// how many days are aggregated per RRA: steps * stepsize * rows / 3600 / 24
// https://oss.oetiker.ch/rrdtool/tut/rrd-beginners.en.html#Understanding_by_an_example

const RRD_VM_DEF: [&CStr; 25] = [
    c"DS:maxcpu:GAUGE:120:0:U",
    c"DS:cpu:GAUGE:120:0:U",
    c"DS:maxmem:GAUGE:120:0:U",
    c"DS:mem:GAUGE:120:0:U",
    c"DS:maxdisk:GAUGE:120:0:U",
    c"DS:disk:GAUGE:120:0:U",
    c"DS:netin:DERIVE:120:0:U",
    c"DS:netout:DERIVE:120:0:U",
    c"DS:diskread:DERIVE:120:0:U",
    c"DS:diskwrite:DERIVE:120:0:U",
    c"DS:memhost:GAUGE:120:0:U",
    c"DS:pressurecpusome:GAUGE:120:0:U",
    c"DS:pressurecpufull:GAUGE:120:0:U",
    c"DS:pressureiosome:GAUGE:120:0:U",
    c"DS:pressureiofull:GAUGE:120:0:U",
    c"DS:pressurememorysome:GAUGE:120:0:U",
    c"DS:pressurememoryfull:GAUGE:120:0:U",
    c"RRA:AVERAGE:0.5:1:1440",    // 1 min * 1440 => 1 day
    c"RRA:AVERAGE:0.5:30:1440",   // 30 min * 1440 => 30 day
    c"RRA:AVERAGE:0.5:360:1440",  // 6 hours * 1440 => 360 day ~1 year
    c"RRA:AVERAGE:0.5:10080:570", // 1 week * 570 => ~10 years
    c"RRA:MAX:0.5:1:1440",        // 1 min * 1440 => 1 day
    c"RRA:MAX:0.5:30:1440",       // 30 min * 1440 => 30 day
    c"RRA:MAX:0.5:360:1440",      // 6 hours * 1440 => 360 day ~1 year
    c"RRA:MAX:0.5:10080:570",     // 1 week * 570 => ~10 years
];

const RRD_NODE_DEF: [&CStr; 27] = [
    c"DS:loadavg:GAUGE:120:0:U",
    c"DS:maxcpu:GAUGE:120:0:U",
    c"DS:cpu:GAUGE:120:0:U",
    c"DS:iowait:GAUGE:120:0:U",
    c"DS:memtotal:GAUGE:120:0:U",
    c"DS:memused:GAUGE:120:0:U",
    c"DS:swaptotal:GAUGE:120:0:U",
    c"DS:swapused:GAUGE:120:0:U",
    c"DS:roottotal:GAUGE:120:0:U",
    c"DS:rootused:GAUGE:120:0:U",
    c"DS:netin:DERIVE:120:0:U",
    c"DS:netout:DERIVE:120:0:U",
    c"DS:memfree:GAUGE:120:0:U",
    c"DS:arcsize:GAUGE:120:0:U",
    c"DS:pressurecpusome:GAUGE:120:0:U",
    c"DS:pressureiosome:GAUGE:120:0:U",
    c"DS:pressureiofull:GAUGE:120:0:U",
    c"DS:pressurememorysome:GAUGE:120:0:U",
    c"DS:pressurememoryfull:GAUGE:120:0:U",
    c"RRA:AVERAGE:0.5:1:1440",    // 1 min * 1440 => 1 day
    c"RRA:AVERAGE:0.5:30:1440",   // 30 min * 1440 => 30 day
    c"RRA:AVERAGE:0.5:360:1440",  // 6 hours * 1440 => 360 day ~1 year
    c"RRA:AVERAGE:0.5:10080:570", // 1 week * 570 => ~10 years
    c"RRA:MAX:0.5:1:1440",        // 1 min * 1440 => 1 day
    c"RRA:MAX:0.5:30:1440",       // 30 min * 1440 => 30 day
    c"RRA:MAX:0.5:360:1440",      // 6 hours * 1440 => 360 day ~1 year
    c"RRA:MAX:0.5:10080:570",     // 1 week * 570 => ~10 years
];

const RRD_STORAGE_DEF: [&CStr; 10] = [
    c"DS:total:GAUGE:120:0:U",
    c"DS:used:GAUGE:120:0:U",
    c"RRA:AVERAGE:0.5:1:1440",    // 1 min * 1440 => 1 day
    c"RRA:AVERAGE:0.5:30:1440",   // 30 min * 1440 => 30 day
    c"RRA:AVERAGE:0.5:360:1440",  // 6 hours * 1440 => 360 day ~1 year
    c"RRA:AVERAGE:0.5:10080:570", // 1 week * 570 => ~10 years
    c"RRA:MAX:0.5:1:1440",        // 1 min * 1440 => 1 day
    c"RRA:MAX:0.5:30:1440",       // 30 min * 1440 => 30 day
    c"RRA:MAX:0.5:360:1440",      // 6 hours * 1440 => 360 day ~1 year
    c"RRA:MAX:0.5:10080:570",     // 1 week * 570 => ~10 years
];

const HELP: &str = "\
proxmox-rrd-migration tool

Migrates existing RRD metrics data to the new format.

Use this only in the process of upgrading from Proxmox VE 8 to 9 according to the upgrade guide!

USAGE:
    proxmox-rrd-migration [OPTIONS]

    FLAGS:
        -h, --help              Prints this help information

    OPTIONS:
        --migrate               Start the migration. Without it, only a dry run will be done.

        --force                 Migrate, even if the target already exists.
                                This will overwrite any migrated RRD files!

        --threads THREADS       Number of paralell threads.

        --source <SOURCE DIR>   Source base directory. Mainly for tests!
                                Default: /var/lib/rrdcached/db

        --target <TARGET DIR>   Target base directory. Mainly for tests!
                                Default: /var/lib/rrdcached/db

        --resources <DIR>       Directory that contains .vmlist and .member files. Mainly for tests!
                                Default: /etc/pve

";

#[derive(Debug)]
struct Args {
    migrate: bool,
    force: bool,
    threads: Option<usize>,
    source: Option<String>,
    target: Option<String>,
    resources: Option<String>,
}

fn parse_args() -> Result<Args, Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{HELP}");
        std::process::exit(0);
    }

    let mut args = Args {
        migrate: false,
        threads: pargs
            .opt_value_from_str("--threads")
            .expect("Could not parse --threads parameter"),
        force: false,
        source: pargs
            .opt_value_from_str("--source")
            .expect("Could not parse --source parameter"),
        target: pargs
            .opt_value_from_str("--target")
            .expect("Could not parse --target parameter"),
        resources: pargs
            .opt_value_from_str("--resources")
            .expect("Could not parse --resources parameter"),
    };

    if pargs.contains("--migrate") {
        args.migrate = true;
    }
    if pargs.contains("--force") {
        args.force = true;
    }

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        bail!(format!("Warning: unused arguments left: {:?}", remaining));
    }

    Ok(args)
}

fn main() {
    let args = match parse_args() {
        Ok(v) => v,
        Err(err) => {
            eprintln!("Error: {err}.");
            std::process::exit(1);
        }
    };

    let source_base_dir = match args.source {
        Some(ref v) => v.as_str(),
        None => BASE_DIR,
    };

    let target_base_dir = match args.target {
        Some(ref v) => v.as_str(),
        None => BASE_DIR,
    };

    let resource_base_dir = match args.resources {
        Some(ref v) => v.as_str(),
        None => RESOURCE_BASE_DIR,
    };

    let source_dir_guests: PathBuf = [source_base_dir, SOURCE_SUBDIR_GUEST].iter().collect();
    let target_dir_guests: PathBuf = [target_base_dir, TARGET_SUBDIR_GUEST].iter().collect();
    let source_dir_nodes: PathBuf = [source_base_dir, SOURCE_SUBDIR_NODE].iter().collect();
    let target_dir_nodes: PathBuf = [target_base_dir, TARGET_SUBDIR_NODE].iter().collect();
    let source_dir_storage: PathBuf = [source_base_dir, SOURCE_SUBDIR_STORAGE].iter().collect();
    let target_dir_storage: PathBuf = [target_base_dir, TARGET_SUBDIR_STORAGE].iter().collect();

    if !args.migrate {
        println!("DRYRUN! Use the --migrate parameter to start the migration.");
    }
    if args.force {
        println!("Force mode! Will overwrite existing target RRD files!");
    }

    if let Err(err) = migrate_nodes(
        source_dir_nodes,
        target_dir_nodes,
        resource_base_dir,
        args.migrate,
        args.force,
    ) {
        eprintln!("Error migrating nodes: {err}");
        std::process::exit(1);
    }
    if let Err(err) = migrate_storage(
        source_dir_storage,
        target_dir_storage,
        args.migrate,
        args.force,
    ) {
        eprintln!("Error migrating storage: {err}");
        std::process::exit(1);
    }
    if let Err(err) = migrate_guests(
        source_dir_guests,
        target_dir_guests,
        resource_base_dir,
        set_threads(&args),
        args.migrate,
        args.force,
    ) {
        eprintln!("Error migrating guests: {err}");
        std::process::exit(1);
    }
}

/// Set number of threads
///
/// Either a fixed parameter or determining a range between 1 to 4 threads
///  based on the number of CPU cores available in the system.
fn set_threads(args: &Args) -> usize {
    if let Some(threads) = args.threads {
        return threads;
    }

    // check for a way to get physical cores and not threads?
    let cpus: usize = match std::process::Command::new("nproc").output() {
        Ok(res) => {
            let nproc_output = res.stdout.as_slice().trim_ascii();
            match String::from_utf8_lossy(nproc_output).parse::<usize>() {
                Ok(cpus) => cpus,
                Err(err) => {
                    eprintln!("failed to parse nproc output, falling back to single CPU – {err}");
                    1
                }
            }
        }
        Err(err) => {
            eprintln!("failed run nproc, falling back to single CPU – {err}");
            1
        }
    };

    if cpus < MAX_AUTO_THREADS * 4 {
        let threads = cpus / 4;
        if threads == 0 {
            return 1;
        }
        return threads;
    }
    MAX_AUTO_THREADS
}

/// Check if a VMID is currently configured
fn resource_present(path: &str, resource: &str) -> Result<bool> {
    let resourcelist = fs::read_to_string(path)?;
    Ok(resourcelist.contains(format!("\"{resource}\"").as_str()))
}

/// Rename file to old, when migrated or resource not present at all -> old RRD file
fn mv_old(file: &str) -> Result<()> {
    let old = format!("{file}.old");
    fs::rename(file, old)?;
    Ok(())
}

/// Colllect all RRD files in the provided directory
fn collect_rrd_files(location: &PathBuf) -> Result<Vec<(CString, OsString)>> {
    let mut files: Vec<(CString, OsString)> = Vec::new();

    fs::read_dir(location)?
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| f.is_file() && f.extension().is_none())
        .for_each(|file| {
            let path = CString::new(file.as_path().as_os_str().as_bytes())
                .expect("Could not convert path to CString.");
            let fname = file
                .file_name()
                .map(|v| v.to_os_string())
                .expect("Could not convert fname to OsString.");
            files.push((path, fname))
        });
    Ok(files)
}

/// Does the actual migration for the given file
fn do_rrd_migration(
    file: File,
    target_location: &Path,
    rrd_def: &[&CStr],
    migrate: bool,
    force: bool,
) -> Result<()> {
    let resource = file.1;
    let mut target_path = target_location.to_path_buf();
    target_path.push(&resource);

    if target_path.exists() && !force {
        println!(
            "already migrated, use --force to overwrite target file: {}",
            target_path.display()
        );
    }

    if !migrate {
        bail!("skipping migration of metrics for {resource:?} - dry-run mode");
    } else if target_path.exists() && !force {
        bail!("refusing to migrate metrics for {resource:?} - target already exists and 'force' not set!");
    }

    let mut source: [*const i8; 2] = [std::ptr::null(); 2];
    source[0] = file.0.as_ptr();

    let target_path = CString::new(target_path.to_str().unwrap()).unwrap();

    unsafe {
        rrd_get_context();
        rrd_clear_error();
        let res = rrd_create_r2(
            target_path.as_ptr(),
            RRD_STEP_SIZE as u64,
            0,
            0,
            source.as_mut_ptr(),
            std::ptr::null(),
            rrd_def.len() as i32,
            rrd_def
                .iter()
                .map(|v| v.as_ptr())
                .collect::<Vec<_>>()
                .as_mut_ptr(),
        );
        if res != 0 {
            bail!(
                "RRD create Error: {}",
                CStr::from_ptr(rrd_get_error()).to_string_lossy()
            );
        }
    }
    Ok(())
}

/// Migrate guest RRD files
///
/// In parallel to speed up the process as most time is spent on converting the
/// data to the new format.
fn migrate_guests(
    source_dir_guests: PathBuf,
    target_dir_guests: PathBuf,
    resources: &str,
    threads: usize,
    migrate: bool,
    force: bool,
) -> Result<(), Error> {
    println!("Migrating RRD metrics data for virtual guests…");
    println!("Using {threads} thread(s)");

    let guest_source_files = collect_rrd_files(&source_dir_guests)?;

    if !target_dir_guests.exists() && migrate {
        println!("Creating new directory: '{}'", target_dir_guests.display());
        std::fs::create_dir(&target_dir_guests)?;
    }

    let total_guests = guest_source_files.len();
    let guests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let guests2 = guests.clone();
    let failed_guests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed_guests2 = failed_guests.clone();
    let start_time = std::time::SystemTime::now();

    let migration_pool = ParallelHandler::new(
        "guest rrd migration",
        threads,
        move |file: (CString, OsString)| {
            let full_path = file.0.clone().into_string().unwrap();

            match do_rrd_migration(
                file,
                &target_dir_guests,
                RRD_VM_DEF.as_slice(),
                migrate,
                force,
            ) {
                Ok(()) => {
                    mv_old(full_path.as_str())?;
                    let current_guests = guests2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if current_guests > 0 && current_guests % 100 == 0 {
                        println!(
                            "migrated metrics for {current_guests} out of {total_guests} guests."
                        );
                    }
                }
                Err(err) => {
                    eprintln!("{err}"); // includes information messages, so just print.
                    failed_guests2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            Ok(())
        },
    );
    let migration_channel = migration_pool.channel();

    for file in guest_source_files {
        let node = file.1.clone().into_string().unwrap();
        if !resource_present(format!("{resources}/.vmlist").as_str(), node.as_str())? {
            println!("VMID: '{node}' not present. Skip and mark as old.");
            mv_old(format!("{}", file.0.to_string_lossy()).as_str())?;
        }
        let migration_channel = migration_channel.clone();
        migration_channel.send(file)?;
    }

    drop(migration_channel);
    migration_pool.complete()?;

    let elapsed = start_time.elapsed()?.as_secs_f64();
    let guests = guests.load(std::sync::atomic::Ordering::SeqCst);

    let failed_guests = failed_guests.load(std::sync::atomic::Ordering::SeqCst);
    if failed_guests == 0 {
        println!("Migrated metrics data of all {guests} guests to new format in {elapsed:.2}s");
    } else {
        println!(
            "Tried to migrated metrics of all guests to new format in {elapsed:.2}s, but did not \
            finish {failed_guests} guests - see output above for details."
        );
    }

    Ok(())
}

/// Migrate node RRD files
///
/// In serial as the number of nodes will not be high.
fn migrate_nodes(
    source_dir_nodes: PathBuf,
    target_dir_nodes: PathBuf,
    resources: &str,
    migrate: bool,
    force: bool,
) -> Result<(), Error> {
    println!("Migrating RRD metrics data for nodes…");

    if !target_dir_nodes.exists() && migrate {
        println!("Creating new directory: '{}'", target_dir_nodes.display());
        std::fs::create_dir(&target_dir_nodes)?;
    }

    let node_source_files = collect_rrd_files(&source_dir_nodes)?;

    let mut no_migration_err = true;
    for file in node_source_files {
        let node = file.1.clone().into_string().unwrap();
        let full_path = file.0.clone().into_string().unwrap();
        println!("Node: '{node}'");
        if !resource_present(format!("{resources}/.members").as_str(), node.as_str())? {
            println!("Node: '{node}' not present. Skip and mark as old.");
            mv_old(format!("{}/{node}", file.0.to_string_lossy()).as_str())?;
        }
        match do_rrd_migration(
            file,
            &target_dir_nodes,
            RRD_NODE_DEF.as_slice(),
            migrate,
            force,
        ) {
            Ok(()) => {
                mv_old(full_path.as_str())?;
            }
            Err(err) => {
                eprintln!("{err}"); // includes information messages, so just print.
                no_migration_err = false;
            }
        }
    }

    if no_migration_err {
        println!("Migrated metrics of all nodes to new format");
    } else {
        println!(
            "Tried to migrated metrics of all nodes to new format - see output above for details."
        );
    }

    Ok(())
}

/// Migrate storage RRD files
///
/// In serial as the number of storage will not be that high.
fn migrate_storage(
    source_dir_storage: PathBuf,
    target_dir_storage: PathBuf,
    migrate: bool,
    force: bool,
) -> Result<(), Error> {
    println!("Migrating RRD metrics data for storages…");

    if !target_dir_storage.exists() && migrate {
        println!("Creating new directory: '{}'", target_dir_storage.display());
        std::fs::create_dir(&target_dir_storage)?;
    }

    let mut no_migration_err = true;
    // storage has another layer of directories per node over which we need to iterate
    fs::read_dir(&source_dir_storage)?
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| f.is_dir())
        .try_for_each(|node| {
            let mut source_storage_subdir = source_dir_storage.clone();
            source_storage_subdir.push(node.file_name().unwrap());

            let mut target_storage_subdir = target_dir_storage.clone();
            target_storage_subdir.push(node.file_name().unwrap());

            if !target_storage_subdir.exists() && migrate {
                fs::create_dir(target_storage_subdir.as_path())?;
                let metadata = target_storage_subdir.metadata()?;
                let mut permissions = metadata.permissions();
                permissions.set_mode(0o755);
                fs::set_permissions(&target_storage_subdir, permissions)?;
            }

            let storage_source_files = collect_rrd_files(&source_storage_subdir)?;
            for file in storage_source_files {
                println!(
                    "Migrating metrics for storage '{}/{}'",
                    node.file_name()
                        .expect("no file name present")
                        .to_string_lossy(),
                    PathBuf::from(file.1.clone()).display()
                );

                let full_path = file.0.clone().into_string().unwrap();
                match do_rrd_migration(
                    file,
                    &target_storage_subdir,
                    RRD_STORAGE_DEF.as_slice(),
                    migrate,
                    force,
                ) {
                    Ok(()) => {
                        mv_old(full_path.as_str())?;
                    }
                    Err(err) => {
                        eprintln!("{err}"); // includes information messages, so just print.
                        no_migration_err = false;
                    }
                }
            }
            Ok::<(), Error>(())
        })?;

    if no_migration_err {
        println!("Migrated metrics of all storages to new format");
    } else {
        println!("Tried to migrated metrics of all storages to new format - see output above for details.");
    }

    Ok(())
}
