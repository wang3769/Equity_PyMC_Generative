async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return await r.json();
}

function fmt(x, digits=4) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return n.toFixed(digits);
}

function pill(label) {
  const cls = (label || "neutral").toLowerCase();
  return `<span class="pill ${cls}">${cls}</span>`;
}

function renderKV(el, obj, order=null) {
  const keys = order || Object.keys(obj);
  el.innerHTML = keys.map(k => `<div class="muted">${k}</div><div>${obj[k] ?? ""}</div>`).join("");
}

function renderTable(headEl, bodyEl, rows) {
  if (!rows.length) {
    headEl.innerHTML = "";
    bodyEl.innerHTML = "<tr><td>No rows</td></tr>";
    return;
  }

  const cols = Object.keys(rows[0]);
  headEl.innerHTML = "<tr>" + cols.map(c => `<th data-col="${c}">${c}</th>`).join("") + "</tr>";

  bodyEl.innerHTML = rows.map(r => {
    return "<tr>" + cols.map(c => {
      if (c === "label") return `<td>${pill(r[c])}</td>`;
      if (["mu_1d","sigma","z_score","p_pos"].includes(c)) return `<td>${fmt(r[c], 6)}</td>`;
      return `<td>${r[c] ?? ""}</td>`;
    }).join("") + "</tr>";
  }).join("");

  // sorting
  let sortCol = null;
  let asc = false;
  headEl.querySelectorAll("th").forEach(th => {
    th.addEventListener("click", () => {
      const c = th.getAttribute("data-col");
      asc = (sortCol === c) ? !asc : false;
      sortCol = c;

      const sorted = [...rows].sort((a,b) => {
        const av = a[c], bv = b[c];
        const an = Number(av), bn = Number(bv);
        const aNum = Number.isFinite(an), bNum = Number.isFinite(bn);
        if (aNum && bNum) return asc ? an - bn : bn - an;
        return asc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
      });
      renderTable(headEl, bodyEl, sorted);
    });
  });
}

async function initIndex() {
  const reportEl = document.getElementById("report");
  const snapEl = document.getElementById("snapshot");
  const headEl = document.getElementById("scoresHead");
  const bodyEl = document.getElementById("scoresBody");
  const searchEl = document.getElementById("search");

  if (!reportEl || !snapEl || !headEl || !bodyEl) return;

  const [report, scores] = await Promise.all([
    getJSON("/api/report"),
    getJSON("/api/scores"),
  ]);

  renderKV(reportEl, {
    asof: report.asof,
    universe_size: report.universe_size,
    train_rows: report.train_rows,
    eval_days: report.eval_days,
    ic_mean: report.ic_mean,
    ic_std: report.ic_std,
    ic_t: report.ic_t,
    notes: report.notes,
  }, ["asof","universe_size","train_rows","eval_days","ic_mean","ic_std","ic_t","notes"]);

  const dt = scores[0]?.dt ?? "";
  const n = scores.length;
  const best = scores[0]?.ticker ?? "";
  renderKV(snapEl, { dt, tickers: n, top_by_z: best }, ["dt","tickers","top_by_z"]);

  renderTable(headEl, bodyEl, scores);

  searchEl?.addEventListener("input", () => {
    const q = (searchEl.value || "").trim().toUpperCase();
    const filtered = q ? scores.filter(r => String(r.ticker).toUpperCase().includes(q)) : scores;
    renderTable(headEl, bodyEl, filtered);
  });
}

async function initModel() {
  const el = document.getElementById("modelCard");
  if (!el) return;
  const data = await getJSON("/api/model");
  el.innerHTML = data.html || "<p>No model card.</p>";
}

window.addEventListener("load", () => {
  initIndex().catch(err => console.error(err));
  initModel().catch(err => console.error(err));
});
