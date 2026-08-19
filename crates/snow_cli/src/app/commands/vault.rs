use super::super::*;

pub(crate) async fn cmd_repair_vault(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.repair_vault().await?;
    print_repair_report(&report);
    Ok(())
}

pub(crate) async fn cmd_rebuild_cache(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.rebuild_cache()?;
    print_rebuild_report(&report);
    Ok(())
}

pub(crate) async fn cmd_verify_vault(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.verify_vault()?;
    print_verification_report(&report);
    Ok(())
}

pub(crate) async fn cmd_prune_orphans(core: &SnowCore, dry_run: bool) -> Result<(), SnowError> {
    let report = core.prune_orphans(dry_run).await?;
    print_prune_report(&report);
    Ok(())
}
