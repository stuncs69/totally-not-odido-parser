const state = {
  field: "",
  filter: "",
  limit: 25,
};

const el = {
  healthDot: document.getElementById("health-dot"),
  healthText: document.getElementById("health-text"),
  field: document.getElementById("field"),
  filter: document.getElementById("filter"),
  limit: document.getElementById("limit"),
  refresh: document.getElementById("refresh"),
  reset: document.getElementById("reset"),
  total: document.getElementById("ana-total"),
  matched: document.getElementById("ana-matched"),
  distinct: document.getElementById("ana-distinct"),
  currentField: document.getElementById("ana-field"),
  distMeta: document.getElementById("dist-meta"),
  distRows: document.getElementById("dist-rows"),
  countField: document.getElementById("count-field"),
  countValue: document.getElementById("count-value"),
  countRun: document.getElementById("count-run"),
  countResult: document.getElementById("count-result"),
};

function number(v) {
  return new Intl.NumberFormat().format(v ?? 0);
}

function percent(v) {
  return `${(v ?? 0).toFixed(2)}%`;
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

function setFieldOptions(fields) {
  const selectedField = state.field;
  const selectedCountField = el.countField.value;

  el.field.innerHTML = "";
  el.countField.innerHTML = "";

  for (const field of fields) {
    const opt = document.createElement("option");
    opt.value = field.name;
    opt.textContent = field.label;
    el.field.appendChild(opt);

    const countOpt = document.createElement("option");
    countOpt.value = field.name;
    countOpt.textContent = field.label;
    el.countField.appendChild(countOpt);
  }

  if (selectedField) {
    el.field.value = selectedField;
  }
  if (!el.field.value && fields.length) {
    const preferred = fields.find((f) => f.name === "type")?.name;
    el.field.value = preferred || fields[0].name;
  }

  if (selectedCountField) {
    el.countField.value = selectedCountField;
  }
  if (!el.countField.value && el.field.value) {
    el.countField.value = el.field.value;
  }

  state.field = el.field.value;
}

async function loadFields() {
  const payload = await api("/api/analytics/fields");
  const fields = payload.fields || [];
  if (!fields.length) {
    throw new Error("no analytics fields available");
  }
  setFieldOptions(fields);
}

function distributionQueryString() {
  const params = new URLSearchParams();
  params.set("field", state.field);
  params.set("limit", String(state.limit));
  if (state.filter) {
    params.set("filter", state.filter);
  }
  return params.toString();
}

function renderDistributionRows(buckets) {
  if (!buckets.length) {
    el.distRows.innerHTML = `<tr><td colspan="5">No matching values</td></tr>`;
    return;
  }

  let maxCount = 1;
  for (const bucket of buckets) {
    if (bucket.count > maxCount) {
      maxCount = bucket.count;
    }
  }

  el.distRows.innerHTML = buckets
    .map((bucket) => {
      const width = Math.max(2, Math.round((bucket.count / maxCount) * 100));
      const encodedValue = encodeURIComponent(bucket.value);
      return `
      <tr>
        <td class="mono">${escapeHTML(bucket.value)}</td>
        <td>${number(bucket.count)}</td>
        <td>${percent(bucket.percentage)}</td>
        <td>
          <div class="bar-track">
            <div class="bar-fill" style="width:${width}%"></div>
          </div>
        </td>
        <td><button class="ghost mini count-link" data-value="${encodedValue}">Count</button></td>
      </tr>`;
    })
    .join("");
}

async function loadDistribution() {
  if (!state.field) {
    return;
  }

  el.distMeta.textContent = "Loading distribution...";
  try {
    const payload = await api(`/api/analytics/distribution?${distributionQueryString()}`);
    el.total.textContent = number(payload.total_rows);
    el.matched.textContent = number(payload.matched_rows);
    el.distinct.textContent = number(payload.distinct_count);
    el.currentField.textContent = payload.field;

    renderDistributionRows(payload.buckets || []);
    el.distMeta.textContent = `Top ${number((payload.buckets || []).length)} of ${number(payload.distinct_count)} distinct values`;
  } catch (err) {
    el.distMeta.textContent = `distribution failed: ${err.message}`;
    el.distRows.innerHTML = `<tr><td colspan="5">Error loading distribution</td></tr>`;
  }
}

async function runCount() {
  const field = el.countField.value;
  const value = el.countValue.value;
  if (!field) {
    el.countResult.textContent = "Select a field first.";
    return;
  }
  if (value === "") {
    el.countResult.textContent = "Enter a value. Use (empty) for blank fields.";
    return;
  }

  el.countResult.textContent = "Counting...";
  try {
    const params = new URLSearchParams({ field, value });
    const payload = await api(`/api/analytics/count?${params.toString()}`);
    el.countResult.textContent = `${number(payload.count)} rows where ${payload.field} = "${payload.value}"`;
  } catch (err) {
    el.countResult.textContent = `count failed: ${err.message}`;
  }
}

function syncStateFromControls() {
  state.field = el.field.value;
  state.filter = el.filter.value.trim();
  state.limit = Number(el.limit.value) || 25;
}

function resetControls() {
  el.filter.value = "";
  el.limit.value = "25";
  state.filter = "";
  state.limit = 25;
}

el.refresh.addEventListener("click", () => {
  syncStateFromControls();
  loadDistribution();
});

el.reset.addEventListener("click", () => {
  resetControls();
  syncStateFromControls();
  loadDistribution();
});

el.field.addEventListener("change", () => {
  syncStateFromControls();
  if (!el.countField.value) {
    el.countField.value = state.field;
  }
  loadDistribution();
});

el.filter.addEventListener("keydown", (evt) => {
  if (evt.key === "Enter") {
    syncStateFromControls();
    loadDistribution();
  }
});

el.limit.addEventListener("change", () => {
  syncStateFromControls();
  loadDistribution();
});

el.countRun.addEventListener("click", () => {
  runCount();
});

el.distRows.addEventListener("click", (evt) => {
  const button = evt.target.closest(".count-link");
  if (!button) {
    return;
  }
  const encodedValue = button.getAttribute("data-value");
  if (!encodedValue) {
    return;
  }
  el.countField.value = el.field.value;
  el.countValue.value = decodeURIComponent(encodedValue);
  runCount();
});

el.countValue.addEventListener("keydown", (evt) => {
  if (evt.key === "Enter") {
    runCount();
  }
});

async function boot() {
  await loadHealth();
  await loadFields();
  syncStateFromControls();
  await loadDistribution();
}

boot().catch((err) => {
  el.distMeta.textContent = `startup failed: ${err.message}`;
});
