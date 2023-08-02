use std::sync::Arc;

use crate::{
    models::{EpochData, SlotData},
    AppResult,
};
use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool};
use tokio::sync::Mutex;

pub async fn table_exists(db_conn: Arc<Mutex<SqlitePool>>, table_name: &str) -> AppResult<bool> {
    let query = r#"SELECT name FROM sqlite_master WHERE type='table' AND name=?"#;

    let result = sqlx::query(query)
        .bind(table_name)
        .fetch_optional(&*db_conn.lock().await)
        .await?;

    Ok(result.is_some())
}

async fn init_default_values(db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<()> {
    let default_epoch = sqlx::query!(
        r#"
            SELECT *
            FROM epoch_data
            WHERE epoch = ?
        "#,
        0
    )
    .fetch_optional(&*db_conn.lock().await)
    .await?;

    let default_slot = sqlx::query!(
        r#"
            SELECT *
            FROM slot_data
            WHERE slot = ?
        "#,
        0
    )
    .fetch_optional(&*db_conn.lock().await)
    .await?;

    let default_epoch_data = EpochData {
        attestationscount: 276,
        attesterslashingscount: 0,
        averagevalidatorbalance: 32006077007,
        blockscount: 32,
        depositscount: 0,
        eligibleether: 674016000000000,
        epoch: 0,
        finalized: true.into(),
        globalparticipationrate: 0.822722315788269,
        missedblocks: 6,
        orphanedblocks: 0,
        proposedblocks: 26,
        proposerslashingscount: 0,
        rewards_exported: true.into(),
        scheduledblocks: 0,
        totalvalidatorbalance: 674144000000000,
        ts: "2020-12-01T12:00:23Z".to_string(),
        validatorscount: 21063,
        voluntaryexitscount: 0,
        votedether: 554528000000000,
        withdrawalcount: 0,
    };
    let default_slot_data = SlotData {
        attestationscount: 0,
        attesterslashingscount: 0,
        blockroot: "0x4d611d5b93fdab69013a7f0a2f961caca0c853f87cfe9595fe50038163079360".to_string(),
        depositscount: 21063,
        epoch: 0,
        eth1data_blockhash: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        eth1data_depositcount: 0,
        eth1data_depositroot: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        exec_base_fee_per_gas: None,
        exec_block_hash: None,
        exec_block_number: None,
        exec_extra_data: None,
        exec_fee_recipient: None,
        exec_gas_limit: None,
        exec_gas_used: None,
        exec_logs_bloom: None,
        exec_parent_hash: None,
        exec_random: None,
        exec_receipts_root: None,
        exec_state_root: None,
        exec_timestamp: None,
        exec_transactions_count: 0,
        graffiti: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        graffiti_text: "".to_string(),
        parentroot: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        proposer: 2147483647,
        proposerslashingscount: 0,
        randaoreveal: "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".to_string(),
        signature: "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".to_string(),
        slot: 0,
        stateroot: "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b".to_string(),
        status: "1".to_string(),
        syncaggregate_bits: None,
        syncaggregate_participation: 0.0,
        syncaggregate_signature: None,
        voluntaryexitscount: 0,
        withdrawalcount: 0
    };

    match (default_epoch.is_some(), default_slot.is_some()) {
        (false, false) => {
            insert_epoch(Arc::clone(&db_conn), default_epoch_data).await?;
            insert_slot(
                Arc::clone(&db_conn),
                default_slot_data.slot,
                &default_slot_data,
            )
            .await?;
        }
        (false, true) => {
            insert_epoch(Arc::clone(&db_conn), default_epoch_data).await?;
        }
        (true, false) => {
            insert_slot(
                Arc::clone(&db_conn),
                default_slot_data.slot,
                &default_slot_data,
            )
            .await?;
        }
        (true, true) => (),
    }

    println!("Continuing operations");

    Ok(())
}

pub async fn setup_db(db_url: &str, db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<()> {
    // let db_conn = db_conn.lock().await;
    if !Sqlite::database_exists(db_url).await.unwrap_or(false) {
        println!("Creating Database: {}", db_url);

        match Sqlite::create_database(db_url).await {
            Ok(_) => println!("DB Created"),
            Err(err) => panic!("error: {}", err),
        }
    } else {
        println!("Using the existing database");

        let slot_table_exists = table_exists(Arc::clone(&db_conn), "slot_data").await?;
        let epoch_table_exists = table_exists(Arc::clone(&db_conn), "epoch_data").await?;

        if slot_table_exists && epoch_table_exists {
            println!("Tables {} & {} already exists", "slot_data", "epoch_data");

            init_default_values(db_conn).await?;
        } else {
            sqlx::migrate!("./migrations")
                .run(&*db_conn.lock().await)
                .await?;
        }
    }

    Ok(())
}

pub async fn insert_epoch(db_conn: Arc<Mutex<SqlitePool>>, epoch_data: EpochData) -> AppResult<()> {
    let existing_epoch = sqlx::query!(
        r#"
            SELECT * 
            FROM epoch_data 
            WHERE epoch = ?
        "#,
        epoch_data.epoch
    )
    .fetch_optional(&*db_conn.lock().await)
    .await?;

    if existing_epoch.is_some() {
        return Ok(());
    } else {
        sqlx::query!(
            r#"
                INSERT INTO epoch_data (
                    attestationscount,
                    attesterslashingscount,
                    averagevalidatorbalance,
                    blockscount,
                    depositscount,
                    eligibleether,
                    epoch,
                    finalized,
                    globalparticipationrate,
                    missedblocks,
                    orphanedblocks,
                    proposedblocks,
                    proposerslashingscount,
                    rewards_exported,
                    scheduledblocks,
                    totalvalidatorbalance,
                    ts,
                    validatorscount,
                    voluntaryexitscount,
                    votedether,
                    withdrawalcount
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            epoch_data.attestationscount,
            epoch_data.attesterslashingscount,
            epoch_data.averagevalidatorbalance,
            epoch_data.blockscount,
            epoch_data.depositscount,
            epoch_data.eligibleether,
            epoch_data.epoch,
            epoch_data.finalized,
            epoch_data.globalparticipationrate,
            epoch_data.missedblocks,
            epoch_data.orphanedblocks,
            epoch_data.proposedblocks,
            epoch_data.proposerslashingscount,
            epoch_data.rewards_exported,
            epoch_data.scheduledblocks,
            epoch_data.totalvalidatorbalance,
            epoch_data.ts,
            epoch_data.validatorscount,
            epoch_data.voluntaryexitscount,
            epoch_data.votedether,
            epoch_data.withdrawalcount
        )
        .execute(&*db_conn.lock().await)
        .await?;
    }
    Ok(())
}

pub async fn update_epoch(
    db_conn: Arc<Mutex<SqlitePool>>,
    epoch_number: i64,
    updated_epoch_data: EpochData,
) -> AppResult<()> {
    let existence_check = sqlx::query!(
        r#"
            SELECT *
            FROM epoch_data 
            WHERE epoch = ?
        "#,
        epoch_number
    )
    .fetch_optional(&*db_conn.lock().await)
    .await?;

    if existence_check.is_some() {
        sqlx::query!(
            r#"
            UPDATE epoch_data
            SET 
                    attestationscount = ?,
                    attesterslashingscount = ?,
                    averagevalidatorbalance = ?,
                    blockscount = ?,
                    depositscount = ?,
                    eligibleether = ?,
                    epoch = ?,
                    finalized = ?,
                    globalparticipationrate = ?,
                    missedblocks = ?,
                    orphanedblocks = ?,
                    proposedblocks = ?,
                    proposerslashingscount = ?,
                    rewards_exported = ?,
                    scheduledblocks = ?,
                    totalvalidatorbalance = ?,
                    ts = ?,
                    validatorscount = ?,
                    voluntaryexitscount = ?,
                    votedether = ?,
                    withdrawalcount = ?
            WHERE epoch = ?
        "#,
            updated_epoch_data.attestationscount,
            updated_epoch_data.attesterslashingscount,
            updated_epoch_data.averagevalidatorbalance,
            updated_epoch_data.blockscount,
            updated_epoch_data.depositscount,
            updated_epoch_data.eligibleether,
            updated_epoch_data.epoch,
            updated_epoch_data.finalized,
            updated_epoch_data.globalparticipationrate,
            updated_epoch_data.missedblocks,
            updated_epoch_data.orphanedblocks,
            updated_epoch_data.proposedblocks,
            updated_epoch_data.proposerslashingscount,
            updated_epoch_data.rewards_exported,
            updated_epoch_data.scheduledblocks,
            updated_epoch_data.totalvalidatorbalance,
            updated_epoch_data.ts,
            updated_epoch_data.validatorscount,
            updated_epoch_data.voluntaryexitscount,
            updated_epoch_data.votedether,
            updated_epoch_data.withdrawalcount,
            epoch_number
        )
        .execute(&*db_conn.lock().await)
        .await?;
    } else {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Epoch does not exist.",
        )));
    }

    Ok(())
}

pub async fn insert_slot(
    db_conn: Arc<Mutex<SqlitePool>>,
    slot_number: i64,
    slot: &SlotData,
) -> AppResult<()> {
    let existing_slot = sqlx::query!(r#"SELECT * FROM slot_data WHERE slot = ?"#, slot_number)
        .fetch_optional(&*db_conn.lock().await)
        .await?;

    if existing_slot.is_some() {
        return Ok(());
    } else {
        sqlx::query!(
            r#"
                INSERT INTO slot_data (
                    attestationscount,
                    attesterslashingscount,
                    blockroot,
                    depositscount,
                    epoch,
                    eth1data_blockhash,
                    eth1data_depositcount,
                    eth1data_depositroot,
                    exec_base_fee_per_gas,
                    exec_block_hash,
                    exec_block_number,
                    exec_extra_data,
                    exec_fee_recipient,
                    exec_gas_limit,
                    exec_gas_used,
                    exec_logs_bloom,
                    exec_parent_hash,
                    exec_random,
                    exec_receipts_root,
                    exec_state_root,
                    exec_timestamp,
                    exec_transactions_count,
                    graffiti,
                    graffiti_text,
                    parentroot,
                    proposer,
                    proposerslashingscount,
                    randaoreveal,
                    signature,
                    slot,
                    stateroot,
                    status,
                    syncaggregate_bits,
                    syncaggregate_participation,
                    syncaggregate_signature,
                    voluntaryexitscount,
                    withdrawalcount
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
                slot.attestationscount,
                slot.attesterslashingscount,
                slot.blockroot,
                slot.depositscount,
                slot.epoch,
                slot.eth1data_blockhash,
                slot.eth1data_depositcount,
                slot.eth1data_depositroot,
                slot.exec_base_fee_per_gas,
                slot.exec_block_hash,
                slot.exec_block_number,
                slot.exec_extra_data,
                slot.exec_fee_recipient,
                slot.exec_gas_limit,
                slot.exec_gas_used,
                slot.exec_logs_bloom,
                slot.exec_parent_hash,
                slot.exec_random,
                slot.exec_receipts_root,
                slot.exec_state_root,
                slot.exec_timestamp,
                slot.exec_transactions_count,
                slot.graffiti,
                slot.graffiti_text,
                slot.parentroot,
                slot.proposer,
                slot.proposerslashingscount,
                slot.randaoreveal,
                slot.signature,
                slot.slot,
                slot.stateroot,
                slot.status,
                slot.syncaggregate_bits,
                slot.syncaggregate_participation,
                slot.syncaggregate_signature,
                slot.voluntaryexitscount,
                slot.withdrawalcount,
        )
        .execute(&*db_conn.lock().await)
        .await?;
    }

    Ok(())
}

pub async fn update_slot(
    db_conn: Arc<Mutex<SqlitePool>>,
    slot_number: i64,
    updated_slot: SlotData,
) -> AppResult<()> {
    let existing_slot = sqlx::query!(
        r#"
            SELECT * 
            FROM slot_data 
            WHERE slot = ?
        "#,
        slot_number
    )
    .fetch_optional(&*db_conn.lock().await)
    .await?;

    if existing_slot.is_some() {
        sqlx::query!(
            r#"
                UPDATE slot_data
                SET attestationscount = ?,
                    attesterslashingscount = ?,
                    blockroot = ?,
                    depositscount = ?,
                    epoch = ?,
                    eth1data_blockhash = ?,
                    eth1data_depositcount = ?,
                    eth1data_depositroot = ?,
                    exec_base_fee_per_gas = ?,
                    exec_block_hash = ?,
                    exec_block_number = ?,
                    exec_extra_data = ?,
                    exec_fee_recipient = ?,
                    exec_gas_limit = ?,
                    exec_gas_used = ?,
                    exec_logs_bloom = ?,
                    exec_parent_hash = ?,
                    exec_random = ?,
                    exec_receipts_root = ?,
                    exec_state_root = ?,
                    exec_timestamp = ?,
                    exec_transactions_count = ?,
                    graffiti = ?,
                    graffiti_text = ?,
                    parentroot = ?,
                    proposer = ?,
                    proposerslashingscount = ?,
                    randaoreveal = ?,
                    signature = ?,
                    stateroot = ?,
                    status = ?,
                    syncaggregate_bits = ?,
                    syncaggregate_participation = ?,
                    syncaggregate_signature = ?,
                    voluntaryexitscount = ?,
                    withdrawalcount = ?
                WHERE slot = ?;
            "#,
            updated_slot.attestationscount,
            updated_slot.attesterslashingscount,
            updated_slot.blockroot,
            updated_slot.depositscount,
            updated_slot.epoch,
            updated_slot.eth1data_blockhash,
            updated_slot.eth1data_depositcount,
            updated_slot.eth1data_depositroot,
            updated_slot.exec_base_fee_per_gas,
            updated_slot.exec_block_hash,
            updated_slot.exec_block_number,
            updated_slot.exec_extra_data,
            updated_slot.exec_fee_recipient,
            updated_slot.exec_gas_limit,
            updated_slot.exec_gas_used,
            updated_slot.exec_logs_bloom,
            updated_slot.exec_parent_hash,
            updated_slot.exec_random,
            updated_slot.exec_receipts_root,
            updated_slot.exec_state_root,
            updated_slot.exec_timestamp,
            updated_slot.exec_transactions_count,
            updated_slot.graffiti,
            updated_slot.graffiti_text,
            updated_slot.parentroot,
            updated_slot.proposer,
            updated_slot.proposerslashingscount,
            updated_slot.randaoreveal,
            updated_slot.signature,
            updated_slot.stateroot,
            updated_slot.status,
            updated_slot.syncaggregate_bits,
            updated_slot.syncaggregate_participation,
            updated_slot.syncaggregate_signature,
            updated_slot.voluntaryexitscount,
            updated_slot.withdrawalcount,
            slot_number
        )
        .execute(&*db_conn.lock().await)
        .await?;

        Ok(())
    } else {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Slot does not exist.",
        )));
    }
}

// Create Models for Database, and handle bool <-> i64 relationship
// of database and dto
pub async fn get_latest_epoch_data(db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<EpochData> {
    let latest_epoch_data = sqlx::query_as!(
        EpochData,
        r#"
            SELECT *
            FROM epoch_data
            ORDER BY epoch DESC
            LIMIT 1;
        "#
    )
    .fetch_one(&*db_conn.lock().await)
    .await?;

    Ok(latest_epoch_data)
}

pub async fn get_latest_unexecuted_slot(db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<SlotData> {
    let latest_slot_data = sqlx::query_as!(
        SlotData,
        r#"
            SELECT *
            FROM slot_data
            WHERE exec_timestamp <> 0
            ORDER BY slot DESC
            LIMIT 1;
        "#
    )
    .fetch_one(&*db_conn.lock().await)
    .await?;

    Ok(latest_slot_data)
}

// pub async fn api_get_specific_slot(db_conn: &SqlitePool, slot_number: i64) -> AppResult<SlotData> {
//     let existing_slot = sqlx::query_as!(
//         SlotData,
//         r#"
//             SELECT *
//             FROM slot_data
//             WHERE slot = ?
//         "#,
//         slot_number
//     )
//     .fetch_optional(db_conn)
//     .await?;
//     if let Some(slot) = existing_slot {
//         Ok(slot)
//     } else {
//         println!("Slot {slot_number} not found in DB, fetching slot from beacon chain...");
//         Ok(utils::external_api::get_specific_slot(db_conn, slot_number)
//             .await?
//             .into())
//     }
// }

// pub async fn api_get_five_recent_epoch_slots(db_conn: &SqlitePool) -> AppResult<Vec<SlotData>> {
//     let one_sixty_slots = sqlx::query_as!(
//         SlotData,
//         r#"
//             SELECT *
//             FROM slot_data
//             ORDER BY slot DESC
//             LIMIT 160;
//         "#
//     )
//     .fetch_all(db_conn)
//     .await?;

//     Ok(one_sixty_slots)
// }
