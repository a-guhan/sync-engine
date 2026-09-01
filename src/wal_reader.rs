use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    env,
    time::Duration,
};

use pg_walstream::{
    CancellationToken, ChangeEvent, ColumnValue, EventType, LogicalReplicationStream,
    PgReplicationConnection, ReplicationError, ReplicationStreamConfig, RetryConfig, RowData,
    StreamingMode, parse_lsn,
};
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

use crate::{AppState, ServerEvent};

#[derive(Clone, Serialize, Deserialize)]
pub struct RawWalEntry {
    pub xid: Option<u32>,
    pub lsn: String,
    pub text: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WalChange {
    pub xid: Option<u32>,
    pub lsn: String,
    pub operation: String,
    pub summary: String,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub old: Option<BTreeMap<String, Option<String>>>,
    pub new: Option<BTreeMap<String, Option<String>>>,
    pub changes: Vec<ColumnChange>,
    pub relations: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ColumnChange {
    pub column: String,
    pub old: Option<String>,
    pub new: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct TransactionState {
    pub changes: Vec<WalChange>,
    pub committed: bool,
}

#[derive(Clone)]
struct Snapshot {
    xmin: u32,
    xmax: u32,
    xip: HashSet<u32>,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

fn replica_db_parts() -> (String, String, String, String, String) {
    (
        env::var("REPLICA_DB_HOST").unwrap_or_else(|_| env_or("DB_HOST", "127.0.0.1")),
        env::var("REPLICA_DB_PORT").unwrap_or_else(|_| "5432".into()),
        env::var("REPLICA_DB_NAME").unwrap_or_else(|_| env_or("DB_NAME", "jus_sync")),
        env::var("REPLICA_DB_USER").unwrap_or_else(|_| env_or("DB_USER", "jus_sync_user")),
        env::var("REPLICA_DB_PASSWORD").unwrap_or_else(|_| env_or("DB_PASSWORD", "jus_sync_pass")),
    )
}

fn logical_replication_url() -> String {
    let (host, port, name, user, password) = replica_db_parts();
    format!("postgresql://{user}:{password}@{host}:{port}/{name}?replication=database")
}

fn logical_slot_name() -> String {
    env_or("REPLICATION_SLOT", "jus_sync_slot")
}

fn raw_replication_url() -> String {
    let (host, port, name, user, password) = replica_db_parts();
    format!("postgresql://{user}:{password}@{host}:{port}/{name}?replication=true")
}

fn replication_config() -> ReplicationStreamConfig {
    ReplicationStreamConfig::builder(
        logical_slot_name(),
        env_or("REPLICATION_PUBLICATION", "jus_sync_pub"),
    )
    .with_protocol_version(2)
    .with_streaming_mode(StreamingMode::On)
    .with_feedback_interval(Duration::from_secs(1))
    .with_connection_timeout(Duration::from_secs(30))
    .with_health_check_interval(Duration::from_secs(30))
    .with_retry_config(RetryConfig::default())
}

fn wal_value(value: &ColumnValue) -> Option<String> {
    match value {
        ColumnValue::Null => None,
        _ => Some(value.to_string()),
    }
}

fn wal_row(row: &RowData) -> BTreeMap<String, Option<String>> {
    row.iter()
        .map(|(name, value)| (name.to_string(), wal_value(value)))
        .collect()
}

fn wal_changes(
    old: Option<&BTreeMap<String, Option<String>>>,
    new: Option<&BTreeMap<String, Option<String>>>,
) -> Vec<ColumnChange> {
    old.into_iter()
        .flat_map(|row| row.keys())
        .chain(new.into_iter().flat_map(|row| row.keys()))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|column| {
            let old = old.and_then(|row| row.get(&column)).cloned().flatten();
            let new = new.and_then(|row| row.get(&column)).cloned().flatten();
            (old != new).then_some(ColumnChange { column, old, new })
        })
        .collect()
}

fn row_text(row: &BTreeMap<String, Option<String>>) -> String {
    row.iter()
        .map(|(key, value)| format!("{key}={}", value.as_deref().unwrap_or("NULL")))
        .collect::<Vec<_>>()
        .join(", ")
}

fn change_text(changes: &[ColumnChange]) -> String {
    changes
        .iter()
        .map(|change| {
            format!(
                "{}: {} -> {}",
                change.column,
                change.old.as_deref().unwrap_or("NULL"),
                change.new.as_deref().unwrap_or("NULL")
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(48)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn logical_entry(event: &ChangeEvent, xid: Option<u32>) -> RawWalEntry {
    let text = match &event.event_type {
        EventType::Begin {
            transaction_id,
            final_lsn,
            ..
        } => {
            format!(
                "begin xid={transaction_id} lsn={} final_lsn={final_lsn}",
                event.lsn
            )
        }
        EventType::Commit {
            commit_lsn,
            end_lsn,
            ..
        } => {
            format!(
                "commit xid={} lsn={} commit_lsn={commit_lsn} end_lsn={end_lsn}",
                xid.unwrap_or_default(),
                event.lsn,
            )
        }
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => {
            format!(
                "insert xid={} lsn={} {schema}.{table} {data:?}",
                xid.unwrap_or_default(),
                event.lsn
            )
        }
        EventType::Update {
            schema,
            table,
            old_data,
            new_data,
            ..
        } => {
            format!(
                "update xid={} lsn={} {schema}.{table} old={old_data:?} new={new_data:?}",
                xid.unwrap_or_default(),
                event.lsn
            )
        }
        EventType::Delete {
            schema,
            table,
            old_data,
            ..
        } => {
            format!(
                "delete xid={} lsn={} {schema}.{table} {old_data:?}",
                xid.unwrap_or_default(),
                event.lsn
            )
        }
        EventType::Truncate(relations) => {
            format!(
                "truncate xid={} lsn={} {relations:?}",
                xid.unwrap_or_default(),
                event.lsn
            )
        }
        other => format!("{other:?}"),
    };
    RawWalEntry {
        xid,
        lsn: event.lsn.to_string(),
        text,
    }
}

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
}

fn parse_physical_frame(data: &[u8]) -> Option<(RawWalEntry, u64)> {
    if data.first() != Some(&b'w') || data.len() < 25 {
        return None;
    }
    let wal_start = be_u64(&data[1..9]);
    let wal_end = be_u64(&data[9..17]);
    let payload = &data[25..];
    let entry = RawWalEntry {
        xid: None,
        lsn: format!("{:X}/{:X}", wal_start >> 32, wal_start as u32),
        text: format!(
            "lsn {} -> {} bytes={}\n{}",
            format!("{:X}/{:X}", wal_start >> 32, wal_start as u32),
            format!("{:X}/{:X}", wal_end >> 32, wal_end as u32),
            payload.len(),
            hex(payload)
        ),
    };
    Some((entry, wal_end))
}

fn keepalive_reply_lsn(data: &[u8]) -> Option<u64> {
    (data.first() == Some(&b'k') && data.len() >= 18).then(|| be_u64(&data[1..9]))
}

fn wal_change(event: &ChangeEvent, xid: Option<u32>) -> Option<WalChange> {
    match &event.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => {
            let new = wal_row(data);
            let target = format!("{schema}.{table}");
            Some(WalChange {
                xid,
                lsn: event.lsn.to_string(),
                operation: "insert".into(),
                summary: format!("Inserted into {target}: {}", row_text(&new)),
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                old: None,
                new: Some(new.clone()),
                changes: wal_changes(None, Some(&new)),
                relations: Vec::new(),
            })
        }
        EventType::Update {
            schema,
            table,
            old_data,
            new_data,
            ..
        } => {
            let old = old_data.as_ref().map(wal_row);
            let new = wal_row(new_data);
            let changes = wal_changes(old.as_ref(), Some(&new));
            let target = format!("{schema}.{table}");
            Some(WalChange {
                xid,
                lsn: event.lsn.to_string(),
                operation: "update".into(),
                summary: format!("Updated {target}: {}", change_text(&changes)),
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                old,
                new: Some(new),
                changes,
                relations: Vec::new(),
            })
        }
        EventType::Delete {
            schema,
            table,
            old_data,
            ..
        } => {
            let old = wal_row(old_data);
            let target = format!("{schema}.{table}");
            Some(WalChange {
                xid,
                lsn: event.lsn.to_string(),
                operation: "delete".into(),
                summary: format!("Deleted from {target}: {}", row_text(&old)),
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                old: Some(old.clone()),
                new: None,
                changes: wal_changes(Some(&old), None),
                relations: Vec::new(),
            })
        }
        EventType::Truncate(relations) => Some(WalChange {
            xid,
            lsn: event.lsn.to_string(),
            operation: "truncate".into(),
            summary: format!(
                "Truncated {}",
                relations
                    .iter()
                    .map(|r| format!("{r:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            schema: None,
            table: None,
            old: None,
            new: None,
            changes: Vec::new(),
            relations: relations
                .iter()
                .map(|relation| format!("{relation:?}"))
                .collect(),
        }),
        _ => None,
    }
}

fn parse_snapshot(snapshot: &str) -> Option<Snapshot> {
    let mut parts = snapshot.splitn(3, ':');
    Some(Snapshot {
        xmin: parts.next()?.parse().ok()?,
        xmax: parts.next()?.parse().ok()?,
        xip: parts
            .next()?
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::parse)
            .collect::<Result<HashSet<_>, _>>()
            .ok()?,
    })
}

fn is_visible(snapshot: &Snapshot, xid: u32) -> bool {
    xid < snapshot.xmin || (xid < snapshot.xmax && !snapshot.xip.contains(&xid))
}

async fn push_raw(state: &AppState, entry: RawWalEntry) {
    let mut raw = state.raw_wal.write().await;
    raw.push(entry.clone());
    if raw.len() > 300 {
        let extra = raw.len() - 300;
        raw.drain(..extra);
    }
    let _ = state.tx.send(ServerEvent::RawWal { entry });
}

async fn push_logical(state: &AppState, entry: RawWalEntry) {
    let mut logical = state.logical_wal.write().await;
    logical.push(entry.clone());
    if logical.len() > 300 {
        let extra = logical.len() - 300;
        logical.drain(..extra);
    }
    let _ = state.tx.send(ServerEvent::LogicalWal { entry });
}

async fn broadcast_transaction(state: &AppState, xid: u32, changes: &[WalChange]) {
    let mut base = state.base_query.write().await;
    let Some(query) = base.as_mut() else {
        return;
    };
    let Some(snapshot) = parse_snapshot(&query.snapshot) else {
        return;
    };
    if is_visible(&snapshot, xid) {
        return;
    }
    for change in changes {
        query.changes.push(change.clone());
        let _ = state.tx.send(ServerEvent::BaseQueryChanged {
            change: change.clone(),
        });
    }
    let _ = state.tx.send(ServerEvent::BaseQuerySet {
        query: query.clone(),
    });
}

async fn push_change(state: &AppState, change: WalChange) {
    if let Some(xid) = change.xid {
        let mut transactions = state.transactions.write().await;
        transactions
            .entry(xid)
            .or_insert_with(|| TransactionState {
                changes: Vec::new(),
                committed: false,
            })
            .changes
            .push(change.clone());
    }
}

fn is_removed_wal_error(error: &ReplicationError) -> bool {
    error.to_string().contains("requested WAL segment")
        && error.to_string().contains("has already been removed")
}

fn reset_logical_slot() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let slot = logical_slot_name();
    let mut conn = PgReplicationConnection::connect(&logical_replication_url())?;
    match conn.drop_replication_slot(&slot, false) {
        Ok(()) => Ok(()),
        Err(error) => {
            let message = error.to_string();
            if message.contains("does not exist")
                || message.contains("replication slot") && message.contains("not found")
            {
                Ok(())
            } else {
                Err(error.into())
            }
        }
    }
}

async fn standby_replay_lsn() -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let (host, port, name, user, password) = replica_db_parts();
    let dsn = format!("host={host} port={port} dbname={name} user={user} password={password}");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let lsn: String = client
        .query_one(
            "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text",
            &[],
        )
        .await?
        .get(0);
    Ok(parse_lsn(&lsn)?)
}

pub async fn run_wal(
    state: AppState,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let mut stream =
            LogicalReplicationStream::new(&logical_replication_url(), replication_config()).await?;
        stream.ensure_replication_slot().await?;

        if let Err(error) = stream.start(None).await {
            if is_removed_wal_error(&error) {
                eprintln!("logical slot fell behind retained WAL; recreating standby logical slot");
                reset_logical_slot()?;
                continue;
            }
            return Err(error.into());
        }

        let mut events = stream.into_stream(cancel.clone());
        let mut current_xid = None;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = events.shutdown().await;
                    return Ok(());
                }
                event = events.next_event() => match event {
                    Ok(event) => {
                        if let EventType::Begin { transaction_id, .. } = &event.event_type {
                            current_xid = Some(*transaction_id);
                            state.transactions.write().await.entry(*transaction_id).or_insert_with(|| TransactionState {
                                changes: Vec::new(),
                                committed: false,
                            });
                        }
                        push_logical(&state, logical_entry(&event, current_xid)).await;
                        if let Some(change) = wal_change(&event, current_xid) {
                            push_change(&state, change).await;
                        }
                        if matches!(event.event_type, EventType::Commit { .. }) {
                            if let Some(xid) = current_xid {
                                let changes = {
                                    let mut transactions = state.transactions.write().await;
                                    match transactions.remove(&xid) {
                                        Some(mut transaction) => {
                                            transaction.committed = true;
                                            transaction.changes
                                        }
                                        None => Vec::new(),
                                    }
                                };
                                broadcast_transaction(&state, xid, &changes).await;
                            }
                            current_xid = None;
                        }
                        let lsn = event.lsn.value();
                        events.update_flushed_lsn(lsn);
                        events.update_applied_lsn(lsn);
                    }
                    Err(ReplicationError::Cancelled(_)) => {
                        let _ = events.shutdown().await;
                        return Ok(());
                    }
                    Err(error) => {
                        let _ = events.shutdown().await;
                        if is_removed_wal_error(&error) {
                            eprintln!("logical slot restart LSN no longer exists; recreating standby logical slot");
                            reset_logical_slot()?;
                            break;
                        }
                        return Err(error.into());
                    },
                }
            }
        }
    }
}

pub async fn run_raw_wal(
    state: AppState,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let slot = env::var("REPLICA_PHYSICAL_REPLICATION_SLOT").ok();
    let mut conn = PgReplicationConnection::connect(&raw_replication_url())?;
    let start_lsn = match slot.as_deref() {
        Some(slot) if !slot.is_empty() => conn
            .read_replication_slot(slot)?
            .restart_lsn
            .map(|lsn| lsn.value())
            .unwrap_or(0),
        _ => standby_replay_lsn().await?,
    };
    conn.start_physical_replication(
        slot.as_deref().filter(|slot| !slot.is_empty()),
        start_lsn,
        None,
    )?;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            frame = conn.get_copy_data_async(&cancel) => match frame {
                Ok(frame) => {
                    if let Some((entry, wal_end)) = parse_physical_frame(&frame) {
                        push_raw(&state, entry).await;
                        conn.send_standby_status_update(wal_end, wal_end, wal_end, false).await?;
                    } else if let Some(wal_end) = keepalive_reply_lsn(&frame) {
                        conn.send_standby_status_update(wal_end, wal_end, wal_end, true).await?;
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}
