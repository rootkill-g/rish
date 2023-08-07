mod db_ops;
mod dtos;
mod models;
mod utils;

use dtos::SlotDataDto;
use once_cell::sync::OnceCell;
use salvo::prelude::*;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::Mutex;
use utils::scheduler;

static SQLITE: OnceCell<SqlitePool> = OnceCell::new();

#[inline]
pub fn get_sqlite() -> &'static SqlitePool {
    unsafe { SQLITE.get_unchecked() }
}

pub type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[handler]
async fn get_specific_slot(req: &mut Request, res: &mut Response) {
    let slot_number = req.query::<i64>("slot").unwrap();
    let data = sqlx::query_as!(
        models::SlotData,
        r#"
            SELECT * 
            FROM slot_data
            WHERE slot = ?
            "#,
        slot_number
    )
    .fetch_optional(get_sqlite())
    .await
    .unwrap();

    let dto: SlotDataDto = data.unwrap().into();

    res.render(serde_json::to_string(&dto).unwrap())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::fmt().init();
    let db_url = env!("DATABASE_URL");
    let pool = SqlitePool::connect(db_url).await?;
    let db_pool = Arc::new(Mutex::new(pool));
    let db_pool_thread_1 = db_pool.clone();
    let db_pool_thread_2 = db_pool.clone();
    // let db_pool_thread_3 = db_pool.clone();

    db_ops::setup_db(db_url, Arc::clone(&db_pool.to_owned())).await?;

    utils::fetch_recent_epoch_slots(Arc::clone(&db_pool), 1).await?;

    println!("Starting scheduler for fetching new epoch data");
    let task1 =
        tokio::spawn(async move { scheduler::fetch_latest_epoch(db_pool_thread_1) }).await?;

    println!("Starting scheduler for updating the current epoch");
    let task2 =
        tokio::spawn(async move { scheduler::update_current_epoch_and_slots(db_pool_thread_2) })
            .await?;

    // println!("Starting the scheduler for updating the unexecuted slot");
    // let task3 =
    //     tokio::spawn(async move { scheduler::update_unexecuted_slot(db_pool_thread_3) }).await?;

    let router = Router::with_path("slot").get(get_specific_slot);
    let acceptor = TcpListener::new("127.0.0.1:5800").bind().await;

    Server::new(acceptor).serve(router).await;

    let (result1, result2) = tokio::join!(task1, task2);

    match result1 {
        Ok(_) => println!("Thread 1 executed successfully"),
        Err(e) => eprintln!("Error in Thread 1 : {e}"),
    }

    match result2 {
        Ok(_) => println!("Thread 2 executed successfully"),
        Err(e) => eprintln!("Error in Thread 2 : {e}"),
    }

    // match result3 {
    //     Ok(_) => println!("Thread 3 executed successfully"),
    //     Err(e) => eprintln!("Error in Thread 3 : {e}"),
    // }

    Ok(())
}
