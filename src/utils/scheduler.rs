use std::{sync::Arc, time::SystemTime};

use super::{external_api::get_specific_epoch_data, get_specific_epoch_slots};
use crate::{db_ops, models::SlotData, AppResult};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use std::time::UNIX_EPOCH;
use tokio::{
    sync::Mutex,
    time::{self, Duration},
};

pub async fn fetch_latest_epoch(db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<()> {
    println!("SCHEDULER_1: Started");

    let interval = Duration::from_secs(384);

    println!("SCHEDULER_1: Fetching latest Epoch Data from Chain");

    let latest_epoch_on_chain = get_specific_epoch_data("latest").await?;

    println!("SCHEDULER_1: Inserting latest Epoch Data into DB");

    db_ops::insert_epoch(Arc::clone(&db_conn), latest_epoch_on_chain.clone().into()).await?;

    println!("SCHEDULER_1: Fetching latest Epoch Slots from Chain");

    let latest_epoch_slots_on_chain = get_specific_epoch_slots(latest_epoch_on_chain.epoch).await?;

    println!("SCHEDULER_1: Inserting latest Epoch Slots into DB");

    for slot in latest_epoch_slots_on_chain {
        let db_slot: SlotData = slot.into();
        db_ops::insert_slot(Arc::clone(&db_conn), db_slot.slot, &db_slot).await?;
    }

    let latest_epoch_timestamp = DateTime::<Utc>::from_utc(
        DateTime::parse_from_rfc3339(&latest_epoch_on_chain.ts)
            .expect("Failed to parse epoch timestamp")
            .naive_utc(),
        Utc,
    )
    .timestamp();

    println!(
        "SCHEDULER_1: Latest Epoch TimeStamp = {}",
        latest_epoch_timestamp,
    );
    println!(
        "SCHEDULER_1: Current System Timestamp: {}",
        Utc::now().timestamp()
    );

    let time_since_update = SystemTime::now()
        .duration_since(UNIX_EPOCH + Duration::from_secs(latest_epoch_timestamp as u64))
        .unwrap_or_else(|_| Duration::from_secs(0));

    println!(
        "SCHEDULER_1: Time since update: {}",
        time_since_update.as_secs()
    );

    let time_remaining = if time_since_update > interval {
        Duration::from_secs(12)
    } else {
        interval - time_since_update
    };

    println!(
        "SCHEDULER_1: Time Remaining for Next Epoch : {}",
        time_remaining.as_secs()
    );

    println!("SCHEDULER_1: Sleeping");

    time::sleep(time_remaining).await;

    println!("SCHEDULER_1: Awake");

    loop {
        println!("SCHEDULER_1: Fetching latest Epoch Data from Chain");

        let latest_epoch_on_chain = get_specific_epoch_data("latest").await?;

        println!("SCHEDULER_1: Inserting latest Epoch Data in DB");

        db_ops::insert_epoch(Arc::clone(&db_conn), latest_epoch_on_chain.clone().into()).await?;

        println!("SCHEDULER_1: Fetching latest Epoch Slots from Chain");

        let latest_epoch_slots_on_chain =
            get_specific_epoch_slots(latest_epoch_on_chain.epoch).await?;

        println!("SCHEDULER_1: Inserting latest Epoch Slots into DB");

        for slot in latest_epoch_slots_on_chain {
            let db_slot: SlotData = slot.into();
            db_ops::insert_slot(Arc::clone(&db_conn), db_slot.slot, &db_slot).await?;
        }

        let latest_epoch_timestamp = DateTime::<Utc>::from_utc(
            DateTime::parse_from_rfc3339(&latest_epoch_on_chain.ts)
                .expect("Failed to parse epoch timestamp")
                .naive_utc(),
            Utc,
        )
        .timestamp();

        println!(
            "SCHEDULER_1: Latest Epoch TimeStamp = {}",
            latest_epoch_timestamp,
        );

        println!(
            "SCHEDULER_1: Current System TimeStamp = {}",
            Utc::now().timestamp()
        );

        let time_since_update = SystemTime::now()
            .duration_since(UNIX_EPOCH + Duration::from_secs(latest_epoch_timestamp as u64))
            .unwrap_or_else(|_| Duration::from_secs(0));

        println!(
            "SCHEDULER_1: Time since update: {}",
            time_since_update.as_secs()
        );

        let time_remaining = if time_since_update > interval {
            Duration::from_secs(12)
        } else {
            interval - time_since_update
        };

        println!(
            "SCHEDULER_1: Time Remaining for Next Epoch : {}",
            time_remaining.as_secs()
        );

        println!("SCHEDULER_1: Sleeping");

        time::sleep(time_remaining).await;

        println!("SCHEDULER_1: Awake");
    }
}

pub async fn update_current_epoch_and_slots(db_conn: Arc<Mutex<SqlitePool>>) -> AppResult<()> {
    println!("SCHEDULER_2: Started");

    let interval = Duration::from_secs(12);

    println!("SCHEDULER_2: Pulling latest epoch from DB");

    let latest_unexecuted_slot_in_db =
        db_ops::get_latest_unexecuted_slot(Arc::clone(&db_conn)).await?;

    let latest_unexecuted_slot_timestamp = latest_unexecuted_slot_in_db.exec_timestamp.unwrap();

    let time_since_update = SystemTime::now()
        .duration_since(UNIX_EPOCH + Duration::from_secs(latest_unexecuted_slot_timestamp as u64))
        .unwrap_or_else(|_| Duration::from_secs(0));
    let time_remaining = if time_since_update > interval {
        Duration::from_secs(0)
    } else {
        interval - time_since_update
    };

    println!("SCHEDULER_2: Sleeping");

    time::sleep(time_remaining).await;

    println!("SCHEDULER_2: Awake");

    loop {
        println!("SCHEDULER_2: Pulling latest epoch from DB");

        let current_epoch_data_in_db = db_ops::get_latest_epoch_data(Arc::clone(&db_conn)).await?;
        let current_epoch_number = current_epoch_data_in_db.epoch;

        println!("SCHEDULER_2: Fetching Epoch {current_epoch_number} Data from chain");
        let updated_current_epoch_data =
            get_specific_epoch_data(&format!("{current_epoch_number}")).await?;

        println!("SCHEDULER_2: Fetching Epoch {current_epoch_number} Slots from chain");

        let updated_current_epoch_slots = get_specific_epoch_slots(current_epoch_number).await?;

        println!("SCHEDULER_2: Updating the Epoch {current_epoch_number} in DB");

        db_ops::update_epoch(
            Arc::clone(&db_conn),
            current_epoch_number,
            updated_current_epoch_data.into(),
        )
        .await?;

        println!("SCHEDULER_2: Updating the slots for Epoch {current_epoch_number} into the DB");

        for slot in updated_current_epoch_slots {
            db_ops::update_slot(Arc::clone(&db_conn), slot.slot, slot.clone().into()).await?;
        }

        println!("SCHEDULER_2: Sleeping");

        time::sleep(interval).await;

        println!("SCHEDULER_2: Awake");
    }
}
