use super::{external_api::get_specific_epoch_data, get_specific_slot};
use crate::{
    db_ops, dtos::EpochDataDto, models::EpochData, utils::fetch_recent_epoch_slots, AppResult,
};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tokio::time::{self, Duration};

async fn epoch_data_scheduler(
    db_conn: SqlitePool,
    current_epoch_data: EpochDataDto,
) -> AppResult<()> {
    println!("SCHEDULER EPOCH DATA FETCHER: Scheduler to fetch new epoch is started");
    let interval = Duration::from_secs(384);
    let mut latest_epoch_data = current_epoch_data;
    let mut epoch_number = latest_epoch_data.epoch + 1;

    loop {
        println!("Waiting for epoch {epoch_number} now.");
        println!("Epoch Data Scheduler Sleeping for 384 seconds now");

        time::sleep(interval).await;
        println!("Epoch Data Scheduler is Awake");
        println!("Fetching epoch {epoch_number} from chain");
        latest_epoch_data = get_specific_epoch_data("latest").await?;

        println!("Putting epoch {epoch_number} from chain into database");
        db_ops::db_ops::insert_epoch(&db_conn, latest_epoch_data.into()).await?;

        println!("Latest epoch data is fetched and put into the database.");

        epoch_number = epoch_number + 1;
    }
}

pub async fn new_epoch_data_schedule_runner(db_conn: SqlitePool) -> AppResult<()> {
    println!("Getting latest epoch in database");

    let current_epoch_data = db_ops::get_latest_epoch_data(&db_conn).await?;

    println!("Lastest Epoch in DB is: {}", current_epoch_data.epoch);

    println!("Getting latest epoch on chain");

    let latest_epoch_data = get_specific_epoch_data("latest").await?;
    let latest_epoch_data_for_db: EpochData = latest_epoch_data.clone().into();
    db_ops::insert_epoch(&db_conn, latest_epoch_data_for_db).await?;
    let how_many = latest_epoch_data.epoch - current_epoch_data.epoch;

    println!("Database is {how_many} epoch behind chain");

    match how_many {
        0 => {
            println!("Current epoch in DB and Latest epoch on chain are same");
            println!(
                "Checking the timestamps and fetching the epoch {}",
                current_epoch_data.epoch + 1
            );

            println!("Setting initial interval");
            let positive_time = (12
                + DateTime::<Utc>::from_utc(
                    DateTime::parse_from_rfc3339(&current_epoch_data.ts)
                        .expect("Failed to parse timestamp string")
                        .naive_utc(),
                    Utc,
                )
                .timestamp())
                - Utc::now().timestamp();
            let initial_interval = if positive_time > 0 {
                time::Duration::from_secs(positive_time.try_into().unwrap())
            } else {
                time::Duration::from_secs(0)
            };

            println!("Epoch Data Scheduler Runner is Sleeping");
            time::sleep(initial_interval).await;
            println!("Epoch Data Scheduler Runner is Awake");
            println!("Fetching latest epoch data from chain");
            get_specific_epoch_data("latest").await?;

            println!("Putting latest epoch data from chain into database");
            db_ops::insert_epoch(&db_conn, latest_epoch_data.into()).await?;

            println!("Latest epoch data is fetched and put into the database.");
            println!("Epoch Data Scheduler Runner is Initiating The Scheduler");

            let _ =
                tokio::spawn(async { epoch_data_scheduler(db_conn, current_epoch_data.into()) })
                    .await?;
        }
        1..=4 => {
            println!("Current epoch in DB is {how_many} epoch behind from on chain");
            println!("Fetching {how_many} epochs from chain");

            fetch_recent_epoch_slots(db_conn.clone(), how_many).await?;

            println!("Fetched {how_many} epochs and stored in DB");
            println!("Epoch Data Scheduler Runner is Initiating The Scheduler");

            let _ =
                tokio::spawn(async { epoch_data_scheduler(db_conn, current_epoch_data.into()) })
                    .await?;
        }
        _ => {
            println!("Current epoch in DB is 5/5+ epoch behind from on chain");
            println!("Fetching five recent epochs from chain");

            fetch_recent_epoch_slots(db_conn.clone(), 5).await?;

            let current_epoch_data = db_ops::get_latest_epoch_data(&db_conn).await?;

            println!("Fetched {how_many} epochs and stored in DB");
            println!("Epoch Data Scheduler Runner is Initiating The Scheduler");

            let _ =
                tokio::spawn(async { epoch_data_scheduler(db_conn, current_epoch_data.into()) })
                    .await?;
        }
    }

    Ok(())
}

pub async fn update_current_epoch(db_conn: SqlitePool) -> AppResult<()> {
    println!("SCHEDULER UPDATE EPOCH: Scheduler for updating current epoch is started");
    let interval = Duration::from_secs(12);
    println!("Fetching latest epoch data in database");
    let current_epoch_data = db_ops::get_latest_epoch_data(&db_conn).await?;
    println!("Latest Epoch in DATABASE: {}", current_epoch_data.epoch);
    let mut current_epoch_number = current_epoch_data.epoch;
    println!("Setting initial interval");
    let current_epoch_time = DateTime::<Utc>::from_utc(
        DateTime::parse_from_rfc3339(&current_epoch_data.ts)
            .expect("Failed to parse timestamp string")
            .naive_utc(),
        Utc,
    )
    .timestamp() as u64;
    let current_time = Utc::now().timestamp() as u64;

    println!("Epoch Time Tag: {current_epoch_time}");
    println!("Current Time Tag: {current_time}");
    let positive_time = (12
        + DateTime::<Utc>::from_utc(
            DateTime::parse_from_rfc3339(&current_epoch_data.ts)
                .expect("Failed to parse timestamp string")
                .naive_utc(),
            Utc,
        )
        .timestamp())
        - Utc::now().timestamp();
    let initial_interval = if positive_time > 0 {
        time::Duration::from_secs(positive_time.try_into().unwrap())
    } else {
        time::Duration::from_secs(0)
    };

    println!("SCHEDULER UPDATE EPOCH: is Sleeping");

    time::sleep(initial_interval).await;

    println!("SCHEDULER UPDATE EPOCH: is Awake");
    println!("Fetching epoch {current_epoch_number} data from chain");
    let mut updated_current_epoch_data =
        get_specific_epoch_data(&format!("{current_epoch_number}")).await?;

    println!("Putting updated epoch {current_epoch_number} into database");
    db_ops::update_epoch(
        &db_conn,
        current_epoch_number,
        updated_current_epoch_data.into(),
    )
    .await?;

    println!("Initial update applied to current epoch {current_epoch_number}");

    loop {
        println!("SCHEDULER UPDATE EPOCH: is Sleeping");
        time::sleep(interval).await;
        println!("SCHEDULER UPDATE EPOCH: is Awake");

        println!("Fetching latest epoch data from database");
        let current_epoch_data = db_ops::get_latest_epoch_data(&db_conn).await?;
        current_epoch_number = current_epoch_data.epoch;

        println!("Fetching the updated epoch {current_epoch_number} from chain");
        updated_current_epoch_data =
            get_specific_epoch_data(&format!("{current_epoch_number}")).await?;

        println!("Putting the epoch {current_epoch_number} data from chain into database");
        db_ops::update_epoch(
            &db_conn,
            current_epoch_number,
            updated_current_epoch_data.into(),
        )
        .await?;

        println!("Waiting for the epoch data for epoch {current_epoch_number} to be updated");
    }
}

pub async fn update_unexecuted_slot(db_conn: SqlitePool) -> AppResult<()> {
    println!("Getting latest unexecuted slot in database");
    let latest_slot_in_db = db_ops::get_latest_unexecuted_slot(&db_conn).await?;
    let mut latest_slot_number = latest_slot_in_db.slot;
    println!("Slot {latest_slot_number} is unexecuted, fetching updated slot from chain");
    let latest_slot_ts = latest_slot_in_db.exec_timestamp;
    println!("Setting initial interval");
    let positive_time = 12 - (Utc::now().timestamp() - latest_slot_ts.unwrap());
    let initial_interval = if positive_time > 0 {
        time::Duration::from_secs(positive_time.try_into().unwrap())
    } else {
        time::Duration::from_secs(0)
    };

    println!("SCHEDULER UPDATE SLOT: is Sleeping");
    time::sleep(initial_interval).await;

    println!("SCHEDULER UPDATE SLOT: is Awake");
    latest_slot_number = latest_slot_number + 1;
    println!("Fetching slot {latest_slot_number} from chain");
    let unexecuted_slot_data = get_specific_slot(latest_slot_number).await?;
    let interval = Duration::from_secs(12);

    println!("Putting slot {latest_slot_number} from chain into database");
    db_ops::update_slot(&db_conn, latest_slot_number, unexecuted_slot_data.into()).await?;

    loop {
        println!("Waiting for slot {} now.", latest_slot_number);

        println!("SCHEDULER UPDATE SLOT: is Sleeping now");
        time::sleep(interval).await;
        println!("SCHEDULER UPDATE SLOT: is Awake");
        println!("Fetching updated slot {latest_slot_number} data from chain");
        let next_slot_data = get_specific_slot(latest_slot_number).await?;

        println!("Putting updated slot {latest_slot_number} data from chain into database");
        db_ops::update_slot(&db_conn, latest_slot_number, next_slot_data.into()).await?;

        println!("Latest executed slot data is fetched and updated in the database.");

        latest_slot_number = latest_slot_number + 1;
    }
}
