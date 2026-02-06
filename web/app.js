const state = {
  user: null,
  cycle: null,
  candidates: [],
  probabilities: [],
  selectedPickIds: [],
};

async function api(path, options = {}) {
  const res = await fetch(path, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const isJson = (res.headers.get("content-type") || "").includes("application/json");
  const payload = isJson ? await res.json() : {};
  if (!res.ok) {
    throw new Error(payload.error || `Request failed (${res.status})`);
  }
  return payload;
}

function text(node, value) {
  node.textContent = value;
}

function fmtPct(value) {
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function rankProbById() {
  const byId = {};
  state.probabilities.forEach((row) => {
    byId[row.candidate_id] = row;
  });
  return byId;
}

function renderAuth() {
  const block = document.getElementById("auth-block");
  block.innerHTML = "";
  if (!state.user) {
    const a = document.createElement("a");
    a.href = "/auth/google/start?redirect=/";
    a.textContent = "Sign in with Google";
    a.className = "badge";
    block.appendChild(a);
    return;
  }

  const info = document.createElement("div");
  info.innerHTML = `<strong>${state.user.display_name}</strong><br /><span class="muted">${state.user.current_chips} chips</span>`;
  block.appendChild(info);

  const logout = document.createElement("button");
  logout.type = "button";
  logout.textContent = "Logout";
  logout.onclick = async () => {
    await api("/api/auth/logout", { method: "POST" });
    window.location.reload();
  };
  block.appendChild(logout);
}

function renderCycle() {
  const el = document.getElementById("cycle-meta");
  if (!state.cycle) {
    el.innerHTML = `<div class="muted">No open cycle.</div>`;
    return;
  }

  el.innerHTML = `
    <div><strong>Cycle ID:</strong> ${state.cycle.id}</div>
    <div><strong>Date:</strong> ${state.cycle.cycle_date}</div>
    <div><strong>Status:</strong> ${state.cycle.status}</div>
  `;
}

function togglePick(candidateId) {
  const idx = state.selectedPickIds.indexOf(candidateId);
  if (idx >= 0) {
    state.selectedPickIds.splice(idx, 1);
  } else {
    if (state.selectedPickIds.length >= 10) {
      alert("You can only pick up to 10 links.");
      return;
    }
    state.selectedPickIds.push(candidateId);
  }
  renderCandidates();
  renderPickList();
}

function movePick(index, dir) {
  const target = index + dir;
  if (target < 0 || target >= state.selectedPickIds.length) {
    return;
  }
  const copy = [...state.selectedPickIds];
  const temp = copy[index];
  copy[index] = copy[target];
  copy[target] = temp;
  state.selectedPickIds = copy;
  renderCandidates();
  renderPickList();
}

function renderCandidates() {
  const container = document.getElementById("candidates");
  const byProb = rankProbById();

  if (!state.candidates.length) {
    container.innerHTML = `<div class="muted">No candidate links yet.</div>`;
    return;
  }

  container.innerHTML = "";
  state.candidates.forEach((candidate) => {
    const card = document.createElement("div");
    card.className = "card";

    const checked = state.selectedPickIds.includes(candidate.id);
    const p = byProb[candidate.id] || { market_probability: 0 };

    card.innerHTML = `
      <div class="row">
        <label>
          <input type="checkbox" ${checked ? "checked" : ""} />
          <strong>${candidate.title || candidate.original_url}</strong>
        </label>
        <span class="badge">${fmtPct(p.market_probability)}</span>
      </div>
      <div class="small">
        <a href="${candidate.original_url}" target="_blank" rel="noreferrer">${candidate.original_url}</a>
      </div>
      <div class="small">Domain: ${candidate.domain} | Submitted by: ${candidate.submitted_by_name}</div>
    `;

    const checkbox = card.querySelector("input[type=checkbox]");
    checkbox.addEventListener("change", () => togglePick(candidate.id));

    container.appendChild(card);
  });
}

function renderPickList() {
  const list = document.getElementById("pick-list");
  list.innerHTML = "";
  const byId = Object.fromEntries(state.candidates.map((c) => [c.id, c]));

  state.selectedPickIds.forEach((candidateId, idx) => {
    const candidate = byId[candidateId];
    const li = document.createElement("li");
    li.innerHTML = `
      <div class="row">
        <span>${candidate ? candidate.title || candidate.original_url : candidateId}</span>
        <span class="actions">
          <button type="button" class="ghost" data-dir="-1">Up</button>
          <button type="button" class="ghost" data-dir="1">Down</button>
          <button type="button" class="ghost" data-remove="1">Remove</button>
        </span>
      </div>
    `;

    li.querySelectorAll("button[data-dir]").forEach((button) => {
      button.addEventListener("click", () => movePick(idx, Number(button.dataset.dir)));
    });

    li.querySelector("button[data-remove]").addEventListener("click", () => {
      state.selectedPickIds = state.selectedPickIds.filter((id) => id !== candidateId);
      renderCandidates();
      renderPickList();
    });

    list.appendChild(li);
  });
}

async function loadLeaderboard() {
  const type = document.getElementById("board-type").value;
  const payload = await api(`/api/leaderboard?type=${encodeURIComponent(type)}`);
  const rows = payload.leaderboard || [];
  const container = document.getElementById("leaderboard");

  if (!rows.length) {
    container.innerHTML = `<div class="muted">No leaderboard data yet.</div>`;
    return;
  }

  container.innerHTML = "";
  rows.forEach((row) => {
    const card = document.createElement("div");
    card.className = "card";
    if (type === "curation") {
      card.innerHTML = `<strong>#${row.rank} ${row.display_name}</strong><div class="small">Curation chips: ${row.curation_chips}</div>`;
    } else {
      card.innerHTML = `<strong>#${row.rank} ${row.display_name}</strong><div class="small">${row.account_type} | Chips: ${row.current_chips}</div>`;
    }
    container.appendChild(card);
  });
}

async function loadArchive(q = "", domain = "") {
  const payload = await api(
    `/api/archive/links?q=${encodeURIComponent(q)}&domain=${encodeURIComponent(domain)}`
  );
  const container = document.getElementById("archive-results");
  const rows = payload.results || [];
  if (!rows.length) {
    container.innerHTML = `<div class="muted">No archive results yet.</div>`;
    return;
  }

  container.innerHTML = "";
  rows.forEach((row) => {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <div><strong>${row.title}</strong></div>
      <div class="small">${row.post_date} | ${row.domain}</div>
      <div class="small"><a href="${row.url}" target="_blank" rel="noreferrer">${row.url}</a></div>
    `;
    container.appendChild(card);
  });
}

async function refreshCycleData() {
  const me = await api("/api/me");
  state.user = me.user;
  state.cycle = me.open_cycle;

  renderAuth();
  renderCycle();

  if (!state.cycle) {
    state.candidates = [];
    state.probabilities = [];
    state.selectedPickIds = [];
    renderCandidates();
    renderPickList();
    return;
  }

  const [cands, probs] = await Promise.all([
    api(`/api/cycles/${state.cycle.id}/candidates`),
    api(`/api/cycles/${state.cycle.id}/probabilities`),
  ]);

  state.candidates = cands.candidates || [];
  state.probabilities = probs.probabilities || [];
  renderCandidates();
  renderPickList();
}

function bindEvents() {
  document.getElementById("refresh-btn").addEventListener("click", async () => {
    await refreshCycleData();
  });

  document.getElementById("board-type").addEventListener("change", async () => {
    await loadLeaderboard();
  });

  document.getElementById("submit-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const msg = document.getElementById("submit-msg");
    const url = document.getElementById("submit-url").value.trim();
    const title = document.getElementById("submit-title").value.trim();

    try {
      if (!state.user) {
        throw new Error("Sign in first.");
      }
      await api("/api/submissions/web", {
        method: "POST",
        body: JSON.stringify({
          cycle_id: state.cycle ? state.cycle.id : null,
          url,
          title,
        }),
      });
      text(msg, "Link submitted.");
      document.getElementById("submit-url").value = "";
      document.getElementById("submit-title").value = "";
      await refreshCycleData();
    } catch (err) {
      text(msg, err.message);
    }
  });

  document.getElementById("save-picks-btn").addEventListener("click", async () => {
    if (!state.user || !state.cycle) {
      alert("Sign in and wait for an open cycle.");
      return;
    }
    await api(`/api/cycles/${state.cycle.id}/picks`, {
      method: "PUT",
      body: JSON.stringify({ candidate_ids: state.selectedPickIds }),
    });
    alert("Picks saved.");
  });

  document.getElementById("phone-start").addEventListener("submit", async (event) => {
    event.preventDefault();
    const msg = document.getElementById("phone-msg");
    try {
      if (!state.user) {
        throw new Error("Sign in first.");
      }
      const phoneNumber = document.getElementById("phone-number").value.trim();
      const payload = await api("/api/phones/link/start", {
        method: "POST",
        body: JSON.stringify({ phone_number: phoneNumber }),
      });
      let info = `Challenge created: ${payload.challenge_id}`;
      if (payload.dev_code) {
        info += ` | Dev code: ${payload.dev_code}`;
      }
      text(msg, info);
      document.getElementById("challenge-id").value = payload.challenge_id;
    } catch (err) {
      text(msg, err.message);
    }
  });

  document.getElementById("phone-verify").addEventListener("submit", async (event) => {
    event.preventDefault();
    const msg = document.getElementById("phone-msg");
    try {
      if (!state.user) {
        throw new Error("Sign in first.");
      }
      const challengeId = document.getElementById("challenge-id").value.trim();
      const code = document.getElementById("phone-code").value.trim();
      await api("/api/phones/link/verify", {
        method: "POST",
        body: JSON.stringify({ challenge_id: challengeId, code }),
      });
      text(msg, "Phone linked successfully.");
    } catch (err) {
      text(msg, err.message);
    }
  });

  document.getElementById("archive-search").addEventListener("submit", async (event) => {
    event.preventDefault();
    const q = document.getElementById("archive-q").value.trim();
    const domain = document.getElementById("archive-domain").value.trim();
    await loadArchive(q, domain);
  });
}

async function main() {
  bindEvents();
  await refreshCycleData();
  await loadLeaderboard();
  await loadArchive();
}

main().catch((err) => {
  alert(`Failed to load app: ${err.message}`);
});
