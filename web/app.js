const state = {
  baseQuery: null,
  execQueries: [],
  rawWal: [],
  logicalWal: [],
};

const baseForm = document.getElementById("base-form");
const baseSql = document.getElementById("base-sql");
const baseSnapshot = document.getElementById("base-snapshot");
const baseSqlView = document.getElementById("base-sql-view");
const resultView = document.getElementById("result-view");
const changesView = document.getElementById("changes-view");
const rawWalView = document.getElementById("raw-wal-view");
const logicalWalView = document.getElementById("logical-wal-view");
const execList = document.getElementById("exec-list");
const addQuery = document.getElementById("add-query");
const execQueryTemplate = document.getElementById("exec-query-template");

function placeholder(text) {
  return `<div class="placeholder">${text}</div>`;
}

function renderTable(rows) {
  if (!rows.length) return placeholder("No rows returned.");
  const columns = Object.keys(rows[0]);
  return `<table><thead><tr>${columns.map((column) => `<th>${column}</th>`).join("")}</tr></thead><tbody>${rows
    .map((row) => `<tr>${columns.map((column) => `<td>${row[column] ?? ""}</td>`).join("")}</tr>`)
    .join("")}</tbody></table>`;
}

function changeText(change) {
  return `${change.operation} ${[change.schema, change.table].filter(Boolean).join(".") || "relation"} xid=${change.xid ?? "-"} lsn=${change.lsn}\n${change.summary}`;
}

function renderLogs(node, items, map, empty) {
  node.innerHTML = items.length
    ? items
        .map((item) => `<pre class="log-entry">${map(item)}</pre>`)
        .join("")
    : placeholder(empty);
}

function renderRawWal(items) {
  rawWalView.innerHTML = items.length
    ? items
        .map((item) => `<pre class="log-entry">${item.text}</pre>`)
        .join("")
    : placeholder("Waiting for raw WAL.");
}

function renderExecQueries() {
  execList.innerHTML = "";
  state.execQueries.forEach((query) => {
    const node = execQueryTemplate.content.firstElementChild.cloneNode(true);
    const sql = node.querySelector(".exec-sql");
    const output = node.querySelector(".exec-output");
    sql.value = query.sql;
    output.textContent = query.output || "";
    sql.addEventListener("input", () => {
      query.sql = sql.value;
    });
    node.querySelector(".run-query").addEventListener("click", async () => {
      await fetch(`/api/exec-queries/${query.id}/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: sql.value }),
      });
    });
    node.querySelector(".delete-query").addEventListener("click", async () => {
      await fetch(`/api/exec-queries/${query.id}`, { method: "DELETE" });
    });
    execList.appendChild(node);
  });
  if (!state.execQueries.length) execList.innerHTML = placeholder("Add runnable queries here.");
}

function render() {
  baseSql.value = state.baseQuery?.sql || "";
  baseSnapshot.textContent = state.baseQuery ? `snapshot ${state.baseQuery.snapshot}` : "";
  baseSqlView.textContent = state.baseQuery?.sql || "No subscribed query yet.";
  resultView.innerHTML = renderTable(state.baseQuery?.rows || []);
  renderLogs(changesView, state.baseQuery?.changes || [], changeText, "Waiting for subscription changes.");
  renderRawWal(state.rawWal);
  renderLogs(logicalWalView, state.logicalWal, (entry) => entry.text, "Waiting for logical WAL.");
  renderExecQueries();
}

function upsertExecQuery(query) {
  const index = state.execQueries.findIndex((item) => item.id === query.id);
  if (index === -1) state.execQueries.push(query);
  else state.execQueries[index] = query;
}

async function loadState() {
  const response = await fetch("/api/state");
  const payload = await response.json();
  state.baseQuery = payload.base_query;
  state.execQueries = payload.exec_queries;
  state.rawWal = payload.raw_wal;
  state.logicalWal = payload.logical_wal;
  render();
}

baseForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const sql = baseSql.value.trim();
  if (!sql) return;
  const response = await fetch("/api/base-query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!response.ok) return alert(await response.text());
  state.baseQuery = await response.json();
  render();
});

addQuery.addEventListener("click", async () => {
  const response = await fetch("/api/exec-queries", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql: "BEGIN;\nCOMMIT;" }),
  });
  upsertExecQuery(await response.json());
  render();
});

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);
  socket.addEventListener("message", (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === "snapshot") {
      state.baseQuery = payload.base_query;
      state.execQueries = payload.exec_queries;
      state.rawWal = payload.raw_wal;
      state.logicalWal = payload.logical_wal;
    }
    if (payload.type === "base_query_set") state.baseQuery = payload.query;
    if (payload.type === "base_query_changed" && state.baseQuery) state.baseQuery.changes.push(payload.change);
    if (payload.type === "exec_query_added") upsertExecQuery(payload.query);
    if (payload.type === "exec_query_updated") upsertExecQuery(payload.query);
    if (payload.type === "exec_query_deleted") state.execQueries = state.execQueries.filter((query) => query.id !== payload.id);
    if (payload.type === "raw_wal") {
      state.rawWal.push(payload.entry);
      if (state.rawWal.length > 300) state.rawWal = state.rawWal.slice(-300);
    }
    if (payload.type === "logical_wal") {
      state.logicalWal.push(payload.entry);
      if (state.logicalWal.length > 300) state.logicalWal = state.logicalWal.slice(-300);
    }
    render();
  });
  socket.addEventListener("close", () => window.setTimeout(connect, 1000));
}

loadState();
connect();
