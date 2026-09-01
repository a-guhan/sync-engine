mod wal_reader;

use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use pg_walstream::CancellationToken;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tokio_postgres::{NoTls, Row, SimpleQueryMessage, types::Type};
use tower_http::services::ServeDir;
use wal_reader::{RawWalEntry, TransactionState, WalChange, run_raw_wal, run_wal};

#[derive(Clone)]
pub struct AppState {
    primary_db: String,
    read_db: String,
    base_query: Arc<RwLock<Option<QueryView>>>,
    exec_queries: Arc<RwLock<Vec<ExecQuery>>>,
    raw_wal: Arc<RwLock<Vec<RawWalEntry>>>,
    logical_wal: Arc<RwLock<Vec<RawWalEntry>>>,
    transactions: Arc<RwLock<HashMap<u32, TransactionState>>>,
    next_id: Arc<AtomicU64>,
    tx: broadcast::Sender<ServerEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryView {
    id: u64,
    sql: String,
    snapshot: String,
    rows: Vec<BTreeMap<String, String>>,
    changes: Vec<WalChange>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExecQuery {
    id: u64,
    sql: String,
    output: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    Snapshot {
        base_query: Option<QueryView>,
        exec_queries: Vec<ExecQuery>,
        raw_wal: Vec<RawWalEntry>,
        logical_wal: Vec<RawWalEntry>,
    },
    BaseQuerySet {
        query: QueryView,
    },
    BaseQueryChanged {
        change: WalChange,
    },
    ExecQueryAdded {
        query: ExecQuery,
    },
    ExecQueryUpdated {
        query: ExecQuery,
    },
    ExecQueryDeleted {
        id: u64,
    },
    RawWal {
        entry: RawWalEntry,
    },
    LogicalWal {
        entry: RawWalEntry,
    },
}

#[derive(Deserialize)]
struct SqlPayload {
    sql: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

fn read_sql_url() -> String {
    let host = env::var("REPLICA_DB_HOST").unwrap_or_else(|_| env_or("DB_HOST", "127.0.0.1"));
    let port = env::var("REPLICA_DB_PORT").unwrap_or_else(|_| "5432".into());
    let name = env::var("REPLICA_DB_NAME").unwrap_or_else(|_| env_or("DB_NAME", "jus_sync"));
    let user = env::var("REPLICA_DB_USER").unwrap_or_else(|_| env_or("DB_USER", "jus_sync_user"));
    let password =
        env::var("REPLICA_DB_PASSWORD").unwrap_or_else(|_| env_or("DB_PASSWORD", "jus_sync_pass"));
    format!("host={host} port={port} dbname={name} user={user} password={password}")
}

fn primary_sql_url() -> String {
    let host = env_or("DB_HOST", "127.0.0.1");
    let port = env_or("DB_PORT", "5433");
    let name = env_or("DB_NAME", "jus_sync");
    let user = env_or("DB_USER", "jus_sync_user");
    let password = env_or("DB_PASSWORD", "jus_sync_pass");
    format!("host={host} port={port} dbname={name} user={user} password={password}")
}

fn row_value(row: &Row, index: usize) -> String {
    match *row.columns()[index].type_() {
        Type::BOOL => row.try_get::<_, bool>(index).map(|v| v.to_string()),
        Type::INT2 => row.try_get::<_, i16>(index).map(|v| v.to_string()),
        Type::INT4 => row.try_get::<_, i32>(index).map(|v| v.to_string()),
        Type::INT8 => row.try_get::<_, i64>(index).map(|v| v.to_string()),
        Type::FLOAT4 => row.try_get::<_, f32>(index).map(|v| v.to_string()),
        Type::FLOAT8 => row.try_get::<_, f64>(index).map(|v| v.to_string()),
        Type::JSON | Type::JSONB => row
            .try_get::<_, serde_json::Value>(index)
            .map(|v| v.to_string()),
        _ => row.try_get::<_, String>(index),
    }
    .unwrap_or_else(|_| "<unprintable>".into())
}

fn rows_to_maps(rows: &[Row]) -> Vec<BTreeMap<String, String>> {
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(index, column)| (column.name().to_string(), row_value(row, index)))
                .collect()
        })
        .collect()
}

fn simple_output(messages: Vec<SimpleQueryMessage>) -> String {
    let mut out = Vec::new();
    for message in messages {
        match message {
            SimpleQueryMessage::CommandComplete(count) => out.push(count.to_string()),
            SimpleQueryMessage::Row(row) => out.push(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join(" | "),
            ),
            _ => {}
        }
    }
    if out.is_empty() {
        "ok".into()
    } else {
        out.join("\n")
    }
}

async fn db_client(dsn: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn ensure_active_db(dsn: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = db_client(dsn)
        .await
        .map_err(|error| format!("database is not reachable: {error}"))?;
    client
        .query_one("SELECT current_database(), 1", &[])
        .await
        .map_err(|error| format!("database is not active: {error}"))?;
    Ok(())
}

async fn ensure_primary_setup(dsn: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = db_client(dsn).await?;
    client
        .batch_execute(
            "
            ALTER TABLE IF EXISTS public.user_table REPLICA IDENTITY FULL;
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'jus_sync_pub') THEN
                CREATE PUBLICATION jus_sync_pub FOR TABLE public.user_table;
              ELSIF NOT EXISTS (
                SELECT 1
                FROM pg_publication_tables
                WHERE pubname = 'jus_sync_pub'
                  AND schemaname = 'public'
                  AND tablename = 'user_table'
              ) THEN
                ALTER PUBLICATION jus_sync_pub ADD TABLE public.user_table;
              END IF;
            END
            $$;
            ",
        )
        .await?;
    let _ = client
        .simple_query("SELECT pg_log_standby_snapshot()")
        .await;
    Ok(())
}

async fn run_query(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<(String, Vec<Row>), (axum::http::StatusCode, String)> {
    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(internal_error)?;
    let result = async {
        let snapshot = client
            .query_one("SELECT pg_current_snapshot()::text", &[])
            .await
            .map_err(internal_error)?
            .get::<_, String>(0);
        let rows = client.query(sql, &[]).await.map_err(internal_error)?;
        client
            .batch_execute("COMMIT")
            .await
            .map_err(internal_error)?;
        Ok::<_, (axum::http::StatusCode, String)>((snapshot, rows))
    }
    .await;
    if result.is_err() {
        let _ = client.batch_execute("ROLLBACK").await;
    }
    result
}

async fn snapshot(State(state): State<AppState>) -> Json<ServerEvent> {
    Json(ServerEvent::Snapshot {
        base_query: state.base_query.read().await.clone(),
        exec_queries: state.exec_queries.read().await.clone(),
        raw_wal: state.raw_wal.read().await.clone(),
        logical_wal: state.logical_wal.read().await.clone(),
    })
}

async fn set_base_query(
    State(state): State<AppState>,
    Json(payload): Json<SqlPayload>,
) -> Result<Json<QueryView>, (axum::http::StatusCode, String)> {
    let client = db_client(&state.read_db).await.map_err(internal_error)?;
    let (snapshot, rows) = run_query(&client, &payload.sql).await?;
    let query = QueryView {
        id: 1,
        sql: payload.sql,
        snapshot,
        rows: rows_to_maps(&rows),
        changes: Vec::new(),
    };
    *state.base_query.write().await = Some(query.clone());
    let _ = state.tx.send(ServerEvent::BaseQuerySet {
        query: query.clone(),
    });
    Ok(Json(query))
}

async fn list_exec_queries(State(state): State<AppState>) -> Json<Vec<ExecQuery>> {
    Json(state.exec_queries.read().await.clone())
}

async fn create_exec_query(
    State(state): State<AppState>,
    Json(payload): Json<SqlPayload>,
) -> Json<ExecQuery> {
    let query = ExecQuery {
        id: state.next_id.fetch_add(1, Ordering::Relaxed),
        sql: payload.sql,
        output: String::new(),
    };
    state.exec_queries.write().await.push(query.clone());
    let _ = state.tx.send(ServerEvent::ExecQueryAdded {
        query: query.clone(),
    });
    Json(query)
}

async fn run_exec_query(
    Path(id): Path<u64>,
    State(state): State<AppState>,
    Json(payload): Json<SqlPayload>,
) -> Result<Json<ExecQuery>, (axum::http::StatusCode, String)> {
    {
        let mut queries = state.exec_queries.write().await;
        let query = queries
            .iter_mut()
            .find(|query| query.id == id)
            .ok_or((axum::http::StatusCode::NOT_FOUND, "query not found".into()))?;
        query.sql = payload.sql.clone();
    }
    let client = db_client(&state.primary_db).await.map_err(internal_error)?;
    let output = client
        .simple_query(&payload.sql)
        .await
        .map(simple_output)
        .map_err(internal_error)?;
    let mut queries = state.exec_queries.write().await;
    let query = queries
        .iter_mut()
        .find(|query| query.id == id)
        .ok_or((axum::http::StatusCode::NOT_FOUND, "query not found".into()))?;
    query.output = output;
    let query = query.clone();
    let _ = state.tx.send(ServerEvent::ExecQueryUpdated {
        query: query.clone(),
    });
    Ok(Json(query))
}

async fn delete_exec_query(
    Path(id): Path<u64>,
    State(state): State<AppState>,
) -> Result<axum::http::StatusCode, (axum::http::StatusCode, String)> {
    let mut queries = state.exec_queries.write().await;
    let len = queries.len();
    queries.retain(|query| query.id != id);
    if queries.len() == len {
        return Err((axum::http::StatusCode::NOT_FOUND, "query not found".into()));
    }
    let _ = state.tx.send(ServerEvent::ExecQueryDeleted { id });
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let snapshot = ServerEvent::Snapshot {
        base_query: state.base_query.read().await.clone(),
        exec_queries: state.exec_queries.read().await.clone(),
        raw_wal: state.raw_wal.read().await.clone(),
        logical_wal: state.logical_wal.read().await.clone(),
    };
    if send_event(&mut socket, &snapshot).await.is_err() {
        return;
    }
    let mut rx = state.tx.subscribe();
    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(event) => {
                    if send_event(&mut socket, &event).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None => return,
                Some(Ok(_)) => {}
                Some(Err(_)) => return,
            }
        }
    }
}

async fn send_event(socket: &mut WebSocket, event: &ServerEvent) -> Result<(), axum::Error> {
    socket
        .send(Message::Text(serde_json::to_string(event).unwrap().into()))
        .await
}

fn internal_error(error: impl ToString) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        error.to_string(),
    )
}

async fn shutdown(cancel: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    cancel.cancel();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let primary_db = primary_sql_url();
    let read_db = read_sql_url();
    ensure_active_db(&primary_db).await?;
    ensure_primary_setup(&primary_db).await?;
    ensure_active_db(&read_db).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let (tx, _) = broadcast::channel(256);
    let state = AppState {
        primary_db,
        read_db,
        base_query: Arc::new(RwLock::new(None)),
        exec_queries: Arc::new(RwLock::new(Vec::new())),
        raw_wal: Arc::new(RwLock::new(Vec::new())),
        logical_wal: Arc::new(RwLock::new(Vec::new())),
        transactions: Arc::new(RwLock::new(HashMap::new())),
        next_id: Arc::new(AtomicU64::new(1)),
        tx,
    };

    let cancel = CancellationToken::new();
    let wal = tokio::spawn({
        let state = state.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(error) = run_wal(state, cancel).await {
                eprintln!("{error}");
            }
        }
    });
    let raw_wal = tokio::spawn({
        let state = state.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(error) = run_raw_wal(state, cancel).await {
                eprintln!("{error}");
            }
        }
    });

    let app = Router::new()
        .route("/api/state", get(snapshot))
        .route("/api/base-query", get(snapshot).post(set_base_query))
        .route(
            "/api/exec-queries",
            get(list_exec_queries).post(create_exec_query),
        )
        .route(
            "/api/exec-queries/{id}",
            axum::routing::delete(delete_exec_query),
        )
        .route(
            "/api/exec-queries/{id}/run",
            axum::routing::post(run_exec_query),
        )
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("web").append_index_html_on_directories(true))
        .with_state(state);

    println!("http://127.0.0.1:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown(cancel.clone()))
        .await?;
    cancel.cancel();
    let _ = wal.await;
    let _ = raw_wal.await;
    Ok(())
}
