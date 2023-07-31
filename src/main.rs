mod db_ops;
mod dtos;
mod models;
mod utils;

use sqlx::SqlitePool;
use utils::scheduler;

pub type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> AppResult<()> {
    let db_url = env!("DATABASE_URL");
    let db_conn = SqlitePool::connect(db_url).await?;
    let db_conn_schedule_new_epoch = db_conn.clone();
    let db_conn_schedule_update_epoch = db_conn.clone();
    let db_conn_schedule_update_slot = db_conn.clone();

    db_ops::setup_db(db_url, &db_conn).await?;

    utils::fetch_recent_epoch_slots(db_conn, 1).await?;

    println!("Starting scheduler for fetching new epoch data");
    let handle1 = tokio::spawn(async {
        scheduler::new_epoch_data_schedule_runner(db_conn_schedule_new_epoch)
    })
    .await?;

    println!("Starting scheduler for updating the current epoch");
    let handle2 =
        tokio::spawn(async { scheduler::update_current_epoch(db_conn_schedule_update_epoch) })
            .await?;

    println!("Starting the scheduler for updating the unexecuted slot");
    let handle3 =
        tokio::spawn(async { scheduler::update_unexecuted_slot(db_conn_schedule_update_slot) })
            .await?;

    let _ = tokio::join!(handle1, handle2, handle3);

    Ok(())
}
