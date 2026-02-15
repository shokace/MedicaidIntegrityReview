const fallback = {
  metadata: { state: "ALL" },
  data_health: {
    n_rows: 12543021,
    duplicate_key_rate: 0.0017,
    missingness: {
      BILLING_PROVIDER_NPI_NUM: 0,
      SERVICING_PROVIDER_NPI_NUM: 0.0004,
      HCPCS_CODE: 0,
      CLAIM_FROM_MONTH: 0,
      TOTAL_UNIQUE_BENEFICIARIES: 0,
      TOTAL_CLAIMS: 0,
      TOTAL_PAID: 0
    },
    violations: {
      claims_lt_12_rate: 0,
      paid_negative_rate: 0.0002,
      benef_gt_claims_rate: 0.009
    }
  },
  unit_price: {
    top_suspicious: [
      { HCPCS_CODE: "J3490", claims: 982103, cv: 5.81, suspicion_score: 34.8 },
      { HCPCS_CODE: "A0425", claims: 741882, cv: 5.12, suspicion_score: 32.5 },
      { HCPCS_CODE: "99213", claims: 2231009, cv: 3.1, suspicion_score: 30.9 },
      { HCPCS_CODE: "T1019", claims: 1542832, cv: 2.7, suspicion_score: 28.7 }
    ]
  },
  digits: {
    basis: "UNIT_PAID",
    cents_last1_dist: { 0: 0.196, 1: 0.084, 2: 0.095, 3: 0.104, 4: 0.092, 5: 0.13, 6: 0.077, 7: 0.076, 8: 0.071, 9: 0.075 }
  },
  correlations: {
    TOTAL_UNIQUE_BENEFICIARIES: { TOTAL_CLAIMS: 0.91, TOTAL_PAID: 0.74 },
    TOTAL_CLAIMS: { TOTAL_PAID: 0.82 },
    within_hcpcs_top200: {
      median_ben_claims: 0.86,
      median_ben_paid: 0.63,
      median_claims_paid: 0.71
    }
  },
  temporal: {
    volatility: {
      paid_delta_std: 295238004.1,
      claims_delta_std: 4292032.8,
      bens_delta_std: 514110.4,
      rows_delta_std: 33815.2
    },
    noise_features: {
      acf1_total_paid: 0.92,
      smooth_ratio: 0.08
    }
  },
  heaping: {
    basis: "unit_paid_cents_last2",
    share_on_5c_grid: 0.34,
    share_on_25c_grid: 0.21,
    max_cent_bucket_share: 0.18
  }
};

const fallbackScore = {
  rule: "If 3+ independent signal families fail -> dataset likely synthetic or altered",
  fail_count: 1,
  family_total: 5,
  raw_fail_count: 2,
  verdict: "NOT_FLAGGED_BY_3PLUS_RULE",
  signals: [
    { name: "Reimbursement ratio clustering", failed: false, metrics: { basis: "top_volume_hcpcs", median_cv: 2.4, p90_cv: 4.1 } },
    { name: "Last digit analysis", failed: true, metrics: { basis: "unit_paid", max_abs_dev: 0.07, chi_like: 0.31 } },
    { name: "Correlation structure", failed: false, metrics: { scope: "within_hcpcs_top200_median", ben_claims: 0.74, ben_paid: 0.64, claims_paid: 0.83 } },
    { name: "Temporal noise", failed: false, metrics: { acf1_total_paid: 0.92, smooth_ratio: 0.08 } },
    { name: "Entropy", failed: true, metrics: { basis: "unit_paid", normalized_entropy_last2: 0.89 } },
    { name: "Heaping detection (grid spacing)", failed: false, metrics: { basis: "unit_paid_cents_last2", share_on_5c_grid: 0.34, share_on_25c_grid: 0.21, max_cent_bucket_share: 0.18 } }
  ]
};

const fallbackReportBundle = {
  default_state: "ALL",
  available_states: ["ALL"],
  reports: { ALL: fallback }
};

const fallbackScoreBundle = {
  default_state: "ALL",
  available_states: ["ALL"],
  scores: { ALL: fallbackScore }
};

const fallbackPeerOutlierBundle = {
  default_state: "ALL",
  available_states: ["ALL"],
  methodology: {
    peer_cell: "state x HCPCS_CODE x CLAIM_FROM_MONTH",
    disclaimer: "Provider outlier ranking is a screening signal and is not legal proof of fraud."
  },
  outliers: { ALL: [] }
};

const explanations = {
  "Reimbursement ratio clustering": {
    what: "This checks how much implied payment-per-claim varies inside each procedure code. The metric CV (coefficient of variation) is spread divided by average.",
    why: "When CV is very high across high-volume procedures, it can indicate unstable or mixed generation patterns that need review.",
    thresholds: "Fail thresholds are calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  },
  "Last digit analysis": {
    what: "This checks whether cents ending digits (0 through 9) of implied unit payment are unnaturally imbalanced.",
    why: "Human-edited or scripted values often overuse certain endings. Natural data can still have bias, but extreme skew is suspicious.",
    thresholds: "Fail thresholds are calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  },
  "Correlation structure": {
    what: "This checks whether key fields still move together in expected ways within high-volume HCPCS strata.",
    why: "If related fields become weakly connected after controlling for service mix, data may have been transformed inconsistently.",
    thresholds: "Lower-bound thresholds are calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  },
  "Temporal noise": {
    what: "This checks month-to-month behavior for unrealistic smoothness.",
    why: "Real systems usually show noise and shocks; synthetic series can look too smooth.",
    thresholds: "Joint thresholds are calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  },
  Entropy: {
    what: "This measures variety in last-two-cent endings of implied unit payment using normalized entropy (0 to 1).",
    why: "Lower entropy means too much repetition, which can appear in templated or copied values.",
    thresholds: "Lower-bound threshold is calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  },
  "Heaping detection (grid spacing)": {
    what: "This checks how often implied unit-payment cents land on reimbursement-grid steps (especially 5-cent and 25-cent spacing).",
    why: "Grid-based payment systems create some heaping naturally, but extreme concentration can indicate transformation artifacts.",
    thresholds: "Upper-bound thresholds are calibrated from the null-model baseline. See threshold_* metrics in the signal detail box."
  }
};

const SIGNAL_SECTION_IDS = {
  "Reimbursement ratio clustering": "payment-mechanics",
  "Last digit analysis": "digit-forensics",
  "Correlation structure": "relationships",
  "Temporal noise": "time-series",
  Entropy: "entropy-detail",
  "Heaping detection (grid spacing)": "heaping-detail"
};

const STATE_NAMES = {
  ALL: "United States",
  AL: "Alabama",
  AK: "Alaska",
  AZ: "Arizona",
  AR: "Arkansas",
  CA: "California",
  CO: "Colorado",
  CT: "Connecticut",
  DE: "Delaware",
  DC: "District of Columbia",
  FL: "Florida",
  GA: "Georgia",
  HI: "Hawaii",
  ID: "Idaho",
  IL: "Illinois",
  IN: "Indiana",
  IA: "Iowa",
  KS: "Kansas",
  KY: "Kentucky",
  LA: "Louisiana",
  ME: "Maine",
  MD: "Maryland",
  MA: "Massachusetts",
  MI: "Michigan",
  MN: "Minnesota",
  MS: "Mississippi",
  MO: "Missouri",
  MT: "Montana",
  NE: "Nebraska",
  NV: "Nevada",
  NH: "New Hampshire",
  NJ: "New Jersey",
  NM: "New Mexico",
  NY: "New York",
  NC: "North Carolina",
  ND: "North Dakota",
  OH: "Ohio",
  OK: "Oklahoma",
  OR: "Oregon",
  PA: "Pennsylvania",
  RI: "Rhode Island",
  SC: "South Carolina",
  SD: "South Dakota",
  TN: "Tennessee",
  TX: "Texas",
  UT: "Utah",
  VT: "Vermont",
  VA: "Virginia",
  WA: "Washington",
  WV: "West Virginia",
  WI: "Wisconsin",
  WY: "Wyoming",
  PR: "Puerto Rico",
  GU: "Guam",
  VI: "U.S. Virgin Islands",
  MP: "Northern Mariana Islands",
  AS: "American Samoa",
  UNK: "Unknown State"
};

const FIPS_TO_STATE = {
  "01": "AL",
  "02": "AK",
  "04": "AZ",
  "05": "AR",
  "06": "CA",
  "08": "CO",
  "09": "CT",
  "10": "DE",
  "11": "DC",
  "12": "FL",
  "13": "GA",
  "15": "HI",
  "16": "ID",
  "17": "IL",
  "18": "IN",
  "19": "IA",
  "20": "KS",
  "21": "KY",
  "22": "LA",
  "23": "ME",
  "24": "MD",
  "25": "MA",
  "26": "MI",
  "27": "MN",
  "28": "MS",
  "29": "MO",
  "30": "MT",
  "31": "NE",
  "32": "NV",
  "33": "NH",
  "34": "NJ",
  "35": "NM",
  "36": "NY",
  "37": "NC",
  "38": "ND",
  "39": "OH",
  "40": "OK",
  "41": "OR",
  "42": "PA",
  "44": "RI",
  "45": "SC",
  "46": "SD",
  "47": "TN",
  "48": "TX",
  "49": "UT",
  "50": "VT",
  "51": "VA",
  "53": "WA",
  "54": "WV",
  "55": "WI",
  "56": "WY",
  "60": "AS",
  "66": "GU",
  "69": "MP",
  "72": "PR",
  "78": "VI"
};

const fmtPct = (n) => `${(n * 100).toFixed(2)}%`;
const fmtNum = (n) => Number(n).toLocaleString();
const fmtN = (n) => Number(n).toFixed(3);
const fmtUsd = (n) => `$${Math.round(Number(n) || 0).toLocaleString()}`;
const chartState = new WeakMap();

let activeState = "ALL";
let reportBundle = fallbackReportBundle;
let scoreBundle = fallbackScoreBundle;
let peerOutlierBundle = fallbackPeerOutlierBundle;
let mapPaths = null;
let mapPuertoRicoPath = null;

function getTooltip() {
  let el = document.getElementById("chartTooltip");
  if (!el) {
    el = document.createElement("div");
    el.id = "chartTooltip";
    el.className = "chart-tooltip";
    el.style.display = "none";
    document.body.appendChild(el);
  }
  return el;
}

function ensureReadout(canvas) {
  const id = `${canvas.id || "chart"}Readout`;
  let el = document.getElementById(id);
  if (!el) {
    el = document.createElement("p");
    el.id = id;
    el.className = "chart-readout";
    el.textContent = "Click a bar to see the exact value.";
    canvas.insertAdjacentElement("afterend", el);
  }
  return el;
}

async function loadJSON(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`Missing ${path}`);
  return res.json();
}

async function loadReportBundle() {
  try {
    const bundle = await loadJSON("outputs/json/report_by_state.json");
    if (bundle && bundle.reports) return bundle;
  } catch (_err) {
    // fall through
  }
  const report = await loadJSON("outputs/json/report.json").catch(() => fallback);
  return { default_state: "ALL", available_states: ["ALL"], reports: { ALL: report } };
}

async function loadScoreBundle() {
  try {
    const bundle = await loadJSON("outputs/json/signal_score_by_state.json");
    if (bundle && bundle.scores) return bundle;
  } catch (_err) {
    // fall through
  }
  const score = await loadJSON("outputs/json/signal_score.json").catch(() => fallbackScore);
  return { default_state: "ALL", available_states: ["ALL"], scores: { ALL: score } };
}

async function loadData() {
  const [reports, scores, peerOutliers] = await Promise.all([loadReportBundle(), loadScoreBundle(), loadPeerOutlierBundle()]);
  return { reports, scores, peerOutliers };
}

async function loadPeerOutlierBundle() {
  try {
    const bundle = await loadJSON("outputs/json/provider_peer_outliers_by_state.json");
    if (bundle && bundle.outliers) return bundle;
  } catch (_err) {
    // fall through
  }
  return fallbackPeerOutlierBundle;
}

function row(html) {
  const tr = document.createElement("tr");
  tr.innerHTML = html;
  return tr;
}

function barChart(canvas, values, labels, color, opts = {}) {
  const valueFormatter = opts.valueFormatter || ((v) => String(v));
  const labelColor = opts.labelColor || "#e6eef3";
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  const pad = 36;
  const max = Math.max(...values, 1e-9);
  const barW = (w - pad * 2) / values.length - 12;
  const bars = [];

  ctx.strokeStyle = "#dbe4d7";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad + ((h - pad * 2) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(pad, y);
    ctx.lineTo(w - pad, y);
    ctx.stroke();
  }

  values.forEach((v, i) => {
    const x = pad + i * ((w - pad * 2) / values.length) + 6;
    const bh = ((h - pad * 2) * v) / max;
    const y = h - pad - bh;
    ctx.fillStyle = color;
    ctx.fillRect(x, y, barW, bh);
    ctx.fillStyle = labelColor;
    ctx.font = "12px IBM Plex Mono";
    ctx.fillText(String(labels[i]), x + barW / 2 - 8, h - 14);
    bars.push({ x, y, w: barW, h: bh, label: labels[i], value: v });
  });

  chartState.set(canvas, { bars });
  ensureReadout(canvas);

  if (!canvas.dataset.boundInteractive) {
    const tooltip = getTooltip();
    const findBar = (ev) => {
      const rect = canvas.getBoundingClientRect();
      const sx = canvas.width / rect.width;
      const sy = canvas.height / rect.height;
      const mx = (ev.clientX - rect.left) * sx;
      const my = (ev.clientY - rect.top) * sy;
      const state = chartState.get(canvas);
      if (!state) return null;
      return state.bars.find((b) => mx >= b.x && mx <= b.x + b.w && my >= b.y && my <= b.y + b.h) || null;
    };

    canvas.addEventListener("mousemove", (ev) => {
      const hit = findBar(ev);
      canvas.style.cursor = hit ? "pointer" : "default";
      if (!hit) {
        tooltip.style.display = "none";
        return;
      }
      tooltip.textContent = `${hit.label}: ${valueFormatter(hit.value)}`;
      tooltip.style.left = `${ev.clientX + 12}px`;
      tooltip.style.top = `${ev.clientY + 12}px`;
      tooltip.style.display = "block";
    });

    canvas.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
      canvas.style.cursor = "default";
    });

    canvas.addEventListener("click", (ev) => {
      const hit = findBar(ev);
      if (!hit) return;
      const readout = ensureReadout(canvas);
      readout.textContent = `Selected bar ${hit.label}: ${valueFormatter(hit.value)}`;
    });

    canvas.dataset.boundInteractive = "1";
  }
}

function metricLines(signal) {
  const m = signal.metrics || {};
  return Object.entries(m)
    .map(([k, v]) => `<li><code>${k}</code>: ${typeof v === "number" ? fmtN(v) : String(v)}</li>`)
    .join("");
}

function findingText(signal) {
  const f = signal.failed;
  const name = signal.name;
  if (name === "Reimbursement ratio clustering") {
    return f
      ? "This failed because reimbursement spread metrics were well above threshold, indicating unusually high instability in implied unit payment structure."
      : "This passed because reimbursement spread stayed within the configured tolerance range.";
  }
  if (name === "Last digit analysis") {
    return f
      ? "This failed because implied unit-payment cents endings were heavily imbalanced relative to baseline expectations."
      : "This passed because implied unit-payment cents endings remained within expected tolerance.";
  }
  if (name === "Correlation structure") {
    return f
      ? "This failed because at least one median within-code relationship was weaker than threshold."
      : "This passed because median within-code relationships met the minimum strength criteria.";
  }
  if (name === "Temporal noise") {
    return f
      ? "This failed because the monthly pattern looked too smooth under the model's smoothness criteria."
      : "This passed because the monthly series retained enough natural variation and was not overly smooth.";
  }
  if (name === "Entropy") {
    return f
      ? "This failed because value diversity was low enough to indicate possible repetition beyond expected operational behavior."
      : "This passed because value diversity remained high enough to avoid repetition concerns.";
  }
  if (name === "Heaping detection (grid spacing)") {
    return f
      ? "This failed because unit-payment cents were overly concentrated at reimbursement-grid spacing, beyond the configured tolerance."
      : "This passed because heaping at reimbursement-grid spacing stayed within the configured tolerance.";
  }
  return f ? "This signal failed under configured thresholds." : "This signal passed under configured thresholds.";
}

function renderSignalDetailBox(signalName, ids) {
  const signal = (window.__scoreSignals || []).find((s) => s.name === signalName);
  const statusEl = document.getElementById(ids.status);
  const explainEl = document.getElementById(ids.explain);
  const metricsEl = document.getElementById(ids.metrics);
  const boxEl = document.getElementById(ids.box);
  if (!signal || !statusEl || !explainEl || !metricsEl || !boxEl) return;
  const ref = explanations[signal.name] || { what: "", why: "", thresholds: "" };

  statusEl.innerHTML = `<strong>${signal.failed ? "FAIL" : "PASS"}</strong>`;
  explainEl.innerHTML = `
    <strong>What this means:</strong> ${ref.what}<br>
    <strong>Why this signal exists:</strong> ${ref.why}<br>
    <strong>Threshold used:</strong> ${ref.thresholds}<br>
    <strong>What happened in this dataset:</strong> ${findingText(signal)}
  `;
  metricsEl.innerHTML = metricLines(signal);
  boxEl.classList.remove("failed", "passed");
  boxEl.classList.add(signal.failed ? "failed" : "passed");
}

function renderScore(score) {
  document.getElementById("ruleText").textContent = score.rule || "-";
  const familyTotal = Number(score.family_total ?? 0);
  const familyFailCount = Number(score.fail_count ?? 0);
  const rawFailCount = Number(score.raw_fail_count ?? score.fail_count ?? 0);
  const verdictFailCount = Number.isFinite(rawFailCount) ? rawFailCount : familyFailCount;
  if (familyTotal > 0) {
    document.getElementById("failCount").textContent = `${score.fail_count} of ${familyTotal} families (raw signal fails: ${rawFailCount} of ${(score.signals || []).length})`;
  } else {
    document.getElementById("failCount").textContent = `${score.fail_count} of ${(score.signals || []).length}`;
  }

  const verdictTextEl = document.getElementById("verdictText");
  const verdictLabelEl = document.getElementById("verdictLabel");
  const verdictRaw = score.verdict || "-";
  let verdictLabel = "PASSED";
  let verdictSummary = "PASSED (<3 SIGNALS FAILED)";
  let verdictClass = "";
  if (verdictFailCount === 3) {
    verdictLabel = "INCONCLUSIVE";
    verdictSummary = "INCONCLUSIVE (3 SIGNALS FAILED)";
    verdictClass = "verdict-inconclusive";
  } else if (verdictFailCount > 3 || verdictRaw === "LIKELY_SYNTHETIC_OR_ALTERED") {
    verdictLabel = "SUSPICIOUS";
    verdictSummary = "SUSPICIOUS (>3 SIGNALS FAILED)";
    verdictClass = "verdict-alert";
  } else {
    verdictClass = "verdict-pass";
  }

  verdictTextEl.textContent = verdictSummary;
  verdictLabelEl.textContent = verdictLabel;
  verdictTextEl.classList.remove("verdict-inconclusive", "verdict-alert", "verdict-pass");
  verdictLabelEl.classList.remove("verdict-inconclusive", "verdict-alert", "verdict-pass");
  verdictTextEl.style.color = "";
  verdictLabelEl.style.color = "";
  if (verdictClass) {
    verdictTextEl.classList.add(verdictClass);
    verdictLabelEl.classList.add(verdictClass);
    if (verdictClass === "verdict-inconclusive") {
      verdictTextEl.style.color = "#ffd36a";
      verdictLabelEl.style.color = "#ffd36a";
    } else if (verdictClass === "verdict-alert") {
      verdictTextEl.style.color = "#ff5c5c";
      verdictLabelEl.style.color = "#ff5c5c";
    } else if (verdictClass === "verdict-pass") {
      verdictTextEl.style.color = "#64d492";
      verdictLabelEl.style.color = "#64d492";
    }
  }
  window.__scoreSignals = score.signals || [];

  const cards = document.getElementById("signalCards");
  cards.innerHTML = "";
  (score.signals || []).forEach((s) => {
    const article = document.createElement("article");
    const targetId = SIGNAL_SECTION_IDS[s.name];
    article.className = `card signal-card signal-card-link ${s.failed ? "failed" : "passed"}`;
    article.setAttribute("role", "button");
    article.setAttribute("tabindex", "0");
    article.innerHTML = `
      <div class="signal-head">
        <h3>${s.name}</h3>
        <span class="badge ${s.failed ? "bad" : "good"}">${s.failed ? "FAIL" : "PASS"}</span>
      </div>
    `;
    article.setAttribute("aria-label", `Go to ${s.name} detail`);
    if (targetId) {
      const jump = () => {
        const el = document.getElementById(targetId);
        if (!el) return;
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      };
      article.addEventListener("click", jump);
      article.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          jump();
        }
      });
    }
    cards.appendChild(article);
  });

  renderSignalDetailBox("Reimbursement ratio clustering", {
    box: "signal1Box",
    status: "signal1Status",
    explain: "signal1Explain",
    metrics: "signal1Metrics"
  });
  renderSignalDetailBox("Last digit analysis", {
    box: "signal2Box",
    status: "signal2Status",
    explain: "signal2Explain",
    metrics: "signal2Metrics"
  });
  renderSignalDetailBox("Correlation structure", {
    box: "signal3Box",
    status: "signal3Status",
    explain: "signal3Explain",
    metrics: "signal3Metrics"
  });
  renderSignalDetailBox("Temporal noise", {
    box: "signal4Box",
    status: "signal4Status",
    explain: "signal4Explain",
    metrics: "signal4Metrics"
  });
  renderSignalDetailBox("Entropy", {
    box: "signal5Box",
    status: "signal5Status",
    explain: "signal5Explain",
    metrics: "signal5Metrics"
  });
  renderSignalDetailBox("Heaping detection (grid spacing)", {
    box: "signal6Box",
    status: "signal6Status",
    explain: "signal6Explain",
    metrics: "signal6Metrics"
  });
}

function renderReport(report) {
  const health = report.data_health || fallback.data_health;
  const violations = health.violations || {};
  const missingness = health.missingness || {};

  document.getElementById("rowsAnalyzed").textContent = fmtNum(health.n_rows || 0);
  document.getElementById("dupRate").textContent = fmtPct(health.duplicate_key_rate || 0);

  const checks = document.getElementById("healthChecks");
  checks.innerHTML = "";
  [
    ["Claims below the suppression floor (<12)", violations.claims_lt_12_rate],
    ["Negative payment rows", violations.paid_negative_rate],
    ["Beneficiaries greater than claims", violations.benef_gt_claims_rate]
  ].forEach(([k, v]) => {
    const li = document.createElement("li");
    li.textContent = `${k}: ${fmtPct(v || 0)}`;
    checks.appendChild(li);
  });

  const missBody = document.getElementById("missingnessTable");
  missBody.innerHTML = "";
  Object.entries(missingness).forEach(([k, v]) => {
    missBody.appendChild(row(`<td>${k}</td><td>${fmtPct(v)}</td>`));
  });

  const hcpcsBody = document.getElementById("hcpcsTable");
  hcpcsBody.innerHTML = "";
  const top = report.unit_price?.top_suspicious || fallback.unit_price.top_suspicious;
  top.slice(0, 12).forEach((r) => {
    hcpcsBody.appendChild(
      row(`<td>${r.HCPCS_CODE || "-"}</td><td>${fmtNum(r.claims || 0)}</td><td>${Number(r.cv || 0).toFixed(2)}</td><td>${Number(r.suspicion_score || 0).toFixed(2)}</td>`)
    );
  });

  const corrBody = document.getElementById("corrTable");
  corrBody.innerHTML = "";
  const c = report.correlations || fallback.correlations;
  const pairs = [
    ["Beneficiaries × Claims", c.TOTAL_UNIQUE_BENEFICIARIES?.TOTAL_CLAIMS],
    ["Beneficiaries × Paid", c.TOTAL_UNIQUE_BENEFICIARIES?.TOTAL_PAID],
    ["Claims × Paid", c.TOTAL_CLAIMS?.TOTAL_PAID]
  ];
  const strat = c.within_hcpcs_top200;
  if (strat) {
    pairs.push(["Within top-200 HCPCS median: Beneficiaries × Claims", strat.median_ben_claims]);
    pairs.push(["Within top-200 HCPCS median: Beneficiaries × Paid", strat.median_ben_paid]);
    pairs.push(["Within top-200 HCPCS median: Claims × Paid", strat.median_claims_paid]);
  }
  pairs.forEach(([name, v]) => {
    corrBody.appendChild(row(`<td>${name}</td><td>${Number(v || 0).toFixed(3)}</td>`));
  });

  const digitDist = report.digits?.cents_last1_dist || fallback.digits.cents_last1_dist;
  const digitLabels = Array.from({ length: 10 }, (_, i) => i);
  const digitVals = digitLabels.map((d) => Number(digitDist[d] || digitDist[String(d)] || 0));
  barChart(document.getElementById("digitChart"), digitVals, digitLabels, "#f6b0b0", {
    valueFormatter: (v) => fmtPct(v),
    labelColor: "#eaf2f7"
  });

  const vol = report.temporal?.volatility || fallback.temporal.volatility;
  const vLabels = ["paid", "claims", "beneficiaries"];
  const vVals = [vol.paid_delta_std, vol.claims_delta_std, vol.bens_delta_std].map((x) => Number(x || 0));
  barChart(document.getElementById("volChart"), vVals, vLabels, "#f2a900", {
    valueFormatter: (v) => fmtNum(Math.round(v)),
    labelColor: "#eaf2f7"
  });
}

function stateDisplayName(code) {
  return `${STATE_NAMES[code] || code} (${code})`;
}

function collectAvailableStates() {
  const fromReports = reportBundle.available_states || [];
  const fromScores = scoreBundle.available_states || [];
  const fromOutliers = peerOutlierBundle.available_states || [];
  const fromReportKeys = Object.keys(reportBundle.reports || {});
  const fromScoreKeys = Object.keys(scoreBundle.scores || {});
  const fromOutlierKeys = Object.keys(peerOutlierBundle.outliers || {});

  const merged = new Set([...fromReports, ...fromScores, ...fromOutliers, ...fromReportKeys, ...fromScoreKeys, ...fromOutlierKeys]);
  if (!merged.has("ALL")) merged.add("ALL");

  const ordered = Array.from(merged).filter(Boolean);
  ordered.sort((a, b) => {
    if (a === "ALL") return -1;
    if (b === "ALL") return 1;
    const aName = STATE_NAMES[a] || a;
    const bName = STATE_NAMES[b] || b;
    return aName.localeCompare(bName);
  });
  return ordered;
}

function resolveReportForState(state) {
  return (reportBundle.reports && reportBundle.reports[state]) || reportBundle.reports?.ALL || fallback;
}

function resolveScoreForState(state) {
  return (scoreBundle.scores && scoreBundle.scores[state]) || scoreBundle.scores?.ALL || fallbackScore;
}

function resolvePeerOutliersForState(state) {
  return (peerOutlierBundle.outliers && peerOutlierBundle.outliers[state]) || [];
}

function outlierRiskClass(label) {
  if (label === "HIGH") return "risk-high";
  if (label === "ELEVATED") return "risk-elevated";
  if (label === "WATCH") return "risk-watch";
  return "risk-low";
}

function renderPeerOutliers() {
  const body = document.getElementById("peerOutlierTable");
  const summary = document.getElementById("peerOutlierSummary");
  if (!body || !summary) return;

  const rows = resolvePeerOutliersForState(activeState);
  const method = peerOutlierBundle.methodology || {};
  const scope = activeState === "ALL" ? "national scope" : `${stateDisplayName(activeState)} scope`;
  summary.textContent = `Top providers in ${scope}, ranked by peer-relative outlier score (${method.peer_cell || "state-level provider peers"}). This is an anomaly-screening signal, not legal proof.`;

  body.innerHTML = "";
  if (!rows.length) {
    body.appendChild(row(`<td colspan="9">No provider outliers met the minimum peer-cell and volume thresholds for ${stateDisplayName(activeState)}.</td>`));
    return;
  }

  rows.slice(0, 25).forEach((r) => {
    const risk = String(r.risk_label || "LOW");
    const stateCode = String(r.provider_state || activeState || "UNK");
    body.appendChild(
      row(
        `<td>${fmtNum(r.rank || 0)}</td>
         <td><code>${r.provider_npi || "-"}</code></td>
         <td>${stateCode}</td>
         <td><span class="risk-chip ${outlierRiskClass(risk)}">${risk}</span></td>
         <td>${fmtN(r.outlier_score || 0)}</td>
         <td>${fmtPct(r.share_rows_ge_3sigma || 0)}</td>
         <td>${fmtNum(r.peer_cells_scored || 0)}</td>
         <td>${fmtNum(r.total_claims || 0)}</td>
         <td>${fmtUsd(r.total_paid || 0)}</td>`
      )
    );
  });
}

function updateStateNote() {
  const note = document.getElementById("stateSelectionNote");
  if (!note) return;
  note.textContent = `State: ${stateDisplayName(activeState)}`;
}

function updateMapActiveState() {
  if (mapPaths) {
    mapPaths.classed("active", (d) => (FIPS_TO_STATE[String(d.id).padStart(2, "0")] || "") === activeState);
  }
  if (mapPuertoRicoPath) {
    mapPuertoRicoPath.classed("active", (d) => (FIPS_TO_STATE[String(d.id).padStart(2, "0")] || "") === activeState);
  }
}

function renderActiveState() {
  const report = resolveReportForState(activeState);
  const score = resolveScoreForState(activeState);
  renderReport(report);
  renderScore(score);
  renderPeerOutliers();
  updateStateNote();
  updateMapActiveState();
}

function setupStateSelector(states, defaultState) {
  const select = document.getElementById("stateSelect");
  if (!select) return;
  select.innerHTML = "";
  states.forEach((code) => {
    const opt = document.createElement("option");
    opt.value = code;
    opt.textContent = stateDisplayName(code);
    select.appendChild(opt);
  });
  activeState = states.includes(defaultState) ? defaultState : "ALL";
  select.value = activeState;
  select.addEventListener("change", (ev) => {
    activeState = ev.target.value;
    renderActiveState();
  });
}

async function renderUSMap(states) {
  const container = document.getElementById("usMap");
  if (!container) return;
  container.innerHTML = "";
  mapPuertoRicoPath = null;

  if (!window.d3 || !window.topojson) {
    const note = document.createElement("p");
    note.className = "us-map-note";
    note.textContent = "Interactive map libraries did not load. Use the dropdown to change state.";
    container.appendChild(note);
    return;
  }

  const stateSet = new Set(states.filter((s) => s.length === 2));
  const mapData = await window.d3.json("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json").catch(() => null);
  if (!mapData) {
    const note = document.createElement("p");
    note.className = "us-map-note";
    note.textContent = "Map data could not be loaded. Use the dropdown to change state.";
    container.appendChild(note);
    return;
  }

  const geo = window.topojson.feature(mapData, mapData.objects.states);
  const width = Math.max(container.clientWidth, 920);
  const height = 380;
  const projection = window.d3.geoAlbersUsa().fitSize([width, height], geo);
  const path = window.d3.geoPath(projection);
  const codeFor = (d) => FIPS_TO_STATE[String(d.id).padStart(2, "0")] || "";
  const applyStateClasses = (selection) =>
    selection.attr("class", (d) => {
      const code = codeFor(d);
      const cls = stateSet.has(code) ? "available" : "missing";
      const active = code === activeState ? " active" : "";
      return `us-state ${cls}${active}`;
    });
  const onStateSelect = (code) => {
    if (!code || !stateSet.has(code)) return;
    activeState = code;
    const select = document.getElementById("stateSelect");
    if (select) select.value = code;
    renderActiveState();
  };
  const onMapClick = (_event, d) => {
    onStateSelect(codeFor(d));
  };

  const svg = window.d3.create("svg").attr("viewBox", `0 0 ${width} ${height}`);
  const mainFeatures = geo.features.filter((d) => codeFor(d) !== "PR");
  mapPaths = svg
    .append("g")
    .selectAll("path")
    .data(mainFeatures)
    .join("path")
    .attr("d", path)
    .on("click", onMapClick);
  applyStateClasses(mapPaths);

  mapPaths.append("title").text((d) => {
    const code = codeFor(d);
    return STATE_NAMES[code] || code || "Unknown";
  });

  const prFeature = geo.features.find((d) => codeFor(d) === "PR");
  if (prFeature) {
    const insetW = 138;
    const insetH = 84;
    const insetPad = 12;
    const insetX = width - insetW - insetPad;
    const insetY = height - insetH - insetPad;
    const prProjection = window.d3.geoMercator().fitExtent(
      [
        [10, 14],
        [insetW - 10, insetH - 20]
      ],
      prFeature
    );
    const prPath = window.d3.geoPath(prProjection);
    const inset = svg
      .append("g")
      .attr("class", "pr-inset")
      .attr("transform", `translate(${insetX},${insetY})`);

    inset
      .append("rect")
      .attr("class", "pr-inset-bg")
      .attr("width", insetW)
      .attr("height", insetH)
      .attr("rx", 8)
      .attr("ry", 8);

    mapPuertoRicoPath = inset.append("path").datum(prFeature).attr("d", prPath).on("click", onMapClick);
    applyStateClasses(mapPuertoRicoPath);
    mapPuertoRicoPath.append("title").text("Puerto Rico (PR)");

    inset
      .append("text")
      .attr("x", insetW / 2)
      .attr("y", insetH - 7)
      .attr("text-anchor", "middle")
      .attr("fill", "#cdd9e4")
      .attr("font-size", "10px")
      .attr("font-family", "IBM Plex Mono, monospace")
      .text("Puerto Rico");
  }

  container.appendChild(svg.node());
}

loadData().then(async ({ reports, scores, peerOutliers }) => {
  reportBundle = reports || fallbackReportBundle;
  scoreBundle = scores || fallbackScoreBundle;
  peerOutlierBundle = peerOutliers || fallbackPeerOutlierBundle;

  const states = collectAvailableStates();
  const defaultState = reportBundle.default_state || scoreBundle.default_state || "ALL";
  setupStateSelector(states, defaultState);
  await renderUSMap(states);
  renderActiveState();
});
