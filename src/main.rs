mod db_ops;
mod dtos;
mod models;
mod utils;

// use dtos::SlotDataDto;
// use once_cell::sync::OnceCell;
use salvo::prelude::*;
use sqlx::{sqlite::SqliteArgumentValue, SqlitePool};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use utils::scheduler;

// static SQLITE: OnceCell<SqlitePool> = OnceCell::new();

// #[inline]
// pub fn get_sqlite() -> &'static SqlitePool {
//     unsafe { SQLITE.get_unchecked() }
// }

pub type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[handler]
async fn get_specific_slot(req: &mut Request, res: &mut Response) {
    let slot_number = req.param::<i64>("slot").unwrap();
    println!("API CALLED: Fetching Slot: {slot_number} from records.");
    let slot_dto = db_ops::api_get_specific_slot(
        Arc::new(Mutex::new(
            SqlitePool::connect(env!("DATABASE_URL")).await.unwrap(),
        )),
        slot_number,
    )
    .await
    .unwrap();
    res.render(Json(slot_dto))
}

#[handler]
async fn get_recent_epoch_slots(_req: &mut Request, res: &mut Response) {
    let epoch_number = db_ops::get_latest_epoch_data(Arc::new(Mutex::new(
        SqlitePool::connect(env!("DATABASE_URL")).await.unwrap(),
    )))
    .await
    .unwrap()
    .epoch;
    let slots = utils::external_api::get_specific_epoch_slots(epoch_number)
        .await
        .unwrap();
    res.render(Json(slots))
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

    let (tx, rx) = oneshot::channel();
    let router = Router::new()
        .push(Router::with_path("slot/<slot>").get(get_specific_slot))
        .push(Router::with_path("recent_five").get(get_recent_epoch_slots));
    let acceptor = TcpListener::new("127.0.0.1:5800").bind().await;

    let server = Server::new(acceptor).serve_with_graceful_shutdown(
        router,
        async {
            rx.await.ok();
        },
        None,
    );

    tokio::spawn(server);

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

    let _ = tx.send(());

    Ok(())
}
