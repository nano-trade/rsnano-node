use crate::LmdbEnv;
use anyhow::Context;
use std::fs;
use tracing::info;

pub fn vacuum(env: LmdbEnv) -> anyhow::Result<()> {
    let data_path = env.file_path().parent().unwrap();
    let source_path = data_path.join("data.ldb");
    let backup_path = data_path.join("backup.vacuum.ldb");
    let vacuum_path = data_path.join("vacuumed.ldb");

    info!("Vacuuming database copy in {:?}", data_path);
    info!("This may take a while...");

    env.copy_db(&vacuum_path)?;

    info!("Finalizing");

    fs::rename(&source_path, &backup_path).context("Failed to rename source to backup")?;
    fs::rename(&vacuum_path, &source_path).context("Failed to rename vacuum to source")?;
    fs::remove_file(&backup_path).context("Failed to remove backup file")?;

    info!("Vacuum completed");

    Ok(())
}
