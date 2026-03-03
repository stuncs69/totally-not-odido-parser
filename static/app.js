const state = {
  limit: 50,
  offset: 0,
  total: 0,
};

const el = {
  healthDot: document.getElementById("health-dot"),
  healthText: document.getElementById("health-text"),
  statTotal: document.getElementById("stat-total"),
  statActive: document.getElementById("stat-active"),
  statInactive: document.getElementById("stat-inactive"),
  statLatest: document.getElementById("stat-latest"),
  rows: document.getElementById("rows"),
  resultMeta: document.getElementById("result-meta"),
  pageIndicator: document.getElementById("page-indicator"),
  prev: document.getElementById("prev"),
  next: document.getElementById("next"),
  apply: document.getElementById("apply"),
  reset: document.getElementById("reset"),
  q: document.getElementById("q"),
  type: document.getElementById("type"),
  status: document.getElementById("status"),
  country: document.getElementById("country"),
  active: document.getElementById("active"),
  sort: document.getElementById("sort"),
  detailDialog: document.getElementById("detail-dialog"),
  detailTitle: document.getElementById("detail-title"),
  detailJSON: document.getElementById("detail-json"),
};

function number(v) {
  return new Intl.NumberFormat().format(v ?? 0);
}

function escapeHTML(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    throw new Error(payload.error || `HTTP ${res.status}`);
  }
  return res.json();
}

function queryString() {
  const p = new URLSearchParams();
  const fields = ["q", "type", "status", "country", "active", "sort"];
  for (const field of fields) {
    const value = el[field].value.trim();
    if (value) p.set(field, value);
  }
  p.set("limit", String(state.limit));
  p.set("offset", String(state.offset));
  return p.toString();
}

function setSelectOptions(select, values) {
  for (const value of values) {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = value;
    select.appendChild(opt);
  }
}

async function loadHealth() {
  try {
    const data = await api("/api/health");
    el.healthDot.classList.add("ok");
    el.healthText.textContent = `ready - ${number(data.rows)} indexed`;
  } catch (err) {
    el.healthDot.classList.remove("ok");
    el.healthText.textContent = `backend error: ${err.message}`;
  }
}

async function loadStats() {
  try {
    const stats = await api("/api/stats");
    el.statTotal.textContent = number(stats.total_rows);
    el.statActive.textContent = number(stats.active_rows);
    el.statInactive.textContent = number(stats.inactive_rows);
    el.statLatest.textContent = stats.last_modified_at || "-";
  } catch (err) {
    el.resultMeta.textContent = `stats failed: ${err.message}`;
  }
}

async function loadFacets() {
  try {
    const facets = await api("/api/facets?limit=120");
    setSelectOptions(el.type, facets.types || []);
    setSelectOptions(el.status, facets.statuses || []);
    setSelectOptions(el.country, facets.countries || []);
  } catch (err) {
    el.resultMeta.textContent = `facets failed: ${err.message}`;
  }
}

function renderRows(rows) {
  if (!rows.length) {
    el.rows.innerHTML = `<tr><td colspan="8">No matches</td></tr>`;
    return;
  }

  el.rows.innerHTML = rows
    .map(
      (r) => `
      <tr data-row="${r.row_num}">
        <td>${number(r.row_num)}</td>
        <td>${escapeHTML(r.id)}</td>
        <td>${escapeHTML(r.name)}</td>
        <td>${escapeHTML(r.type)}</td>
        <td>${escapeHTML(r.status)}</td>
        <td>${escapeHTML(r.billing_city)}</td>
        <td>${escapeHTML(r.billing_country)}</td>
        <td>${escapeHTML(r.modified_date)}</td>
      </tr>`
    )
    .join("");
}

function updatePagination() {
  const page = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  el.pageIndicator.textContent = `Page ${page} / ${totalPages}`;
  el.prev.disabled = state.offset <= 0;
  el.next.disabled = state.offset + state.limit >= state.total;
}

async function loadRecords() {
  el.resultMeta.textContent = "Loading records...";
  try {
    const result = await api(`/api/records?${queryString()}`);
    state.total = result.total;
    renderRows(result.records || []);
    const start = state.total ? state.offset + 1 : 0;
    const end = Math.min(state.offset + state.limit, state.total);
    el.resultMeta.textContent = `${number(start)}-${number(end)} of ${number(state.total)}`;
    updatePagination();
  } catch (err) {
    el.resultMeta.textContent = `query failed: ${err.message}`;
    el.rows.innerHTML = `<tr><td colspan="8">Error loading data</td></tr>`;
  }
}

async function openDetail(rowNum) {
  try {
    const detail = await api(`/api/records/${rowNum}`);
    el.detailTitle.textContent = `Record ${number(detail.row_num)}`;
    el.detailJSON.textContent = JSON.stringify(detail.data, null, 2);
    if (typeof el.detailDialog.showModal === "function") {
      el.detailDialog.showModal();
    }
  } catch (err) {
    el.resultMeta.textContent = `detail failed: ${err.message}`;
  }
}

function resetFilters() {
  el.q.value = "";
  el.type.value = "";
  el.status.value = "";
  el.country.value = "";
  el.active.value = "";
  el.sort.value = "";
}

el.apply.addEventListener("click", () => {
  state.offset = 0;
  loadRecords();
});

el.reset.addEventListener("click", () => {
  resetFilters();
  state.offset = 0;
  loadRecords();
});

el.prev.addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  loadRecords();
});

el.next.addEventListener("click", () => {
  if (state.offset + state.limit < state.total) {
    state.offset += state.limit;
    loadRecords();
  }
});

el.rows.addEventListener("click", (evt) => {
  const row = evt.target.closest("tr[data-row]");
  if (!row) return;
  const rowNum = row.getAttribute("data-row");
  if (rowNum) openDetail(rowNum);
});

el.q.addEventListener("keydown", (evt) => {
  if (evt.key === "Enter") {
    state.offset = 0;
    loadRecords();
  }
});

async function boot() {
  await loadHealth();
  await Promise.all([loadStats(), loadFacets()]);
  await loadRecords();
}

boot();
