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
  rule: "If 3+ independent check groups fail, flag for deeper review",
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
    peer_cell: "providers compared to similar providers in the same state",
    disclaimer: "Provider outlier ranking is a screening signal and is not legal proof of fraud."
  },
  outliers: { ALL: [] }
};

const explanations = {
  "Reimbursement ratio clustering": {
    what: "This checks how much payment per claim changes within the same procedure code.",
    why: "Big swings in similar services can be a warning sign.",
    thresholds: "State-specific cutoff values are listed below."
  },
  "Last digit analysis": {
    what: "This checks whether payment cents endings (0 to 9) are too uneven.",
    why: "When just a few endings appear too often, data may be overly processed.",
    thresholds: "State-specific cutoff values are listed below."
  },
  "Correlation structure": {
    what: "This checks whether key numbers still move together in expected ways.",
    why: "If related values stop moving together, the data may have been changed inconsistently.",
    thresholds: "State-specific cutoff values are listed below."
  },
  "Temporal noise": {
    what: "This checks if month-to-month trends look too smooth.",
    why: "Real systems usually have bumps and shocks over time.",
    thresholds: "State-specific cutoff values are listed below."
  },
  Entropy: {
    what: "This measures how much variety appears in the last two cents of payment values.",
    why: "Low variety means too much repetition.",
    thresholds: "State-specific cutoff values are listed below."
  },
  "Heaping detection (grid spacing)": {
    what: "This checks whether values bunch up too much on pricing grid steps (like 5 or 25 cents).",
    why: "Some bunching is normal in healthcare pricing, but too much can be a warning sign.",
    thresholds: "State-specific cutoff values are listed below."
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

const SIGNAL_DISPLAY_NAMES = {
  "Reimbursement ratio clustering": "Payment-per-Claim Spread",
  "Last digit analysis": "Cents Pattern Check",
  "Correlation structure": "Expected Relationships",
  "Temporal noise": "Month-to-Month Movement",
  Entropy: "Repetition Check",
  "Heaping detection (grid spacing)": "Grid-Step Clustering"
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

const METRIC_LABELS = {
  basis: "Measured on",
  scope: "Compared within",
  median_cv: "Typical spread",
  p90_cv: "High-end spread",
  max_abs_dev: "Largest cents imbalance",
  chi_like: "Digit mismatch score",
  ben_claims: "People vs claims relationship",
  ben_paid: "People vs paid amount relationship",
  claims_paid: "Claims vs paid amount relationship",
  acf1_total_paid: "Month-to-month similarity",
  smooth_ratio: "Month-to-month change rate",
  normalized_entropy_last2: "Ending-value variety (0 to 1)",
  share_on_5c_grid: "Values on 5-cent steps",
  share_on_25c_grid: "Values on 25-cent steps",
  max_cent_bucket_share: "Most common cents ending share",
  threshold_median_cv_hi: "Fail if typical spread is above",
  threshold_p90_cv_hi: "Fail if high-end spread is above",
  threshold_max_abs_dev_hi: "Fail if largest cents imbalance is above",
  threshold_chi_like_hi: "Fail if overall cents mismatch is above",
  threshold_ben_claims_lo: "Fail if people vs claims relationship is below",
  threshold_ben_paid_lo: "Fail if people vs paid relationship is below",
  threshold_claims_paid_lo: "Fail if claims vs paid relationship is below",
  threshold_acf1_hi: "Fail if month-to-month similarity is above",
  threshold_smooth_ratio_lo: "Fail if change rate is below",
  threshold_entropy_lo: "Fail if ending-value variety is below",
  threshold_share_on_5c_grid_hi: "Fail if 5-cent clustering is above",
  threshold_share_on_25c_grid_hi: "Fail if 25-cent clustering is above",
  threshold_max_cent_bucket_share_hi: "Fail if most common cents ending share is above"
};

const METRIC_DESCRIPTIONS = {
  basis: "This tells you which values were used for this check.",
  scope: "This tells you which subset of data was compared.",
  median_cv: "How spread out payment per claim is in typical high-volume procedure codes. Lower usually means more stable.",
  p90_cv: "Spread near the high end (90th percentile) of high-volume procedure codes. Higher means more extreme variation.",
  max_abs_dev: "Largest gap between observed and expected share for any one last-digit ending.",
  chi_like: "One combined mismatch score across all last-digit endings. Higher means the overall pattern is less natural.",
  ben_claims: "How strongly people served and claims move together. Closer to 1 means stronger linkage.",
  ben_paid: "How strongly people served and paid amount move together. Closer to 1 means stronger linkage.",
  claims_paid: "How strongly claims and paid amount move together. Closer to 1 means stronger linkage.",
  acf1_total_paid: "How similar this month is to the previous month. Closer to 1 means very similar month to month.",
  smooth_ratio: "How large month-to-month changes are relative to average paid amount. Lower means smoother behavior.",
  normalized_entropy_last2: "Variety score of last-two-cent endings. Closer to 1 means more variety; lower means more repetition.",
  share_on_5c_grid: "Percent of values landing on 5-cent steps (like .00, .05, .10).",
  share_on_25c_grid: "Percent of values landing on 25-cent steps (like .00, .25, .50, .75).",
  max_cent_bucket_share: "Largest share taken by any single last-two-cent ending bucket.",
  threshold_median_cv_hi: "If Typical spread is above this cutoff, this check fails.",
  threshold_p90_cv_hi: "If High-end spread is above this cutoff, this check fails.",
  threshold_max_abs_dev_hi: "If Largest cents imbalance is above this cutoff, this check fails.",
  threshold_chi_like_hi: "If Digit mismatch score is above this cutoff, this check fails.",
  threshold_ben_claims_lo: "If People vs claims relationship is below this cutoff, this check fails.",
  threshold_ben_paid_lo: "If People vs paid amount relationship is below this cutoff, this check fails.",
  threshold_claims_paid_lo: "If Claims vs paid amount relationship is below this cutoff, this check fails.",
  threshold_acf1_hi: "If Month-to-month similarity is above this cutoff, this check fails (too smooth).",
  threshold_smooth_ratio_lo: "If Month-to-month change rate is below this cutoff, this check fails (too smooth).",
  threshold_entropy_lo: "If Ending-value variety is below this cutoff, this check fails.",
  threshold_share_on_5c_grid_hi: "If 5-cent-step share is above this cutoff, this check fails.",
  threshold_share_on_25c_grid_hi: "If 25-cent-step share is above this cutoff, this check fails.",
  threshold_max_cent_bucket_share_hi: "If the most common ending share is above this cutoff, this check fails."
};

const BASIS_LABELS = {
  top_volume_hcpcs: "Payment per claim within high-volume procedure codes",
  unit_paid: "Payment per claim values",
  unit_paid_cents_last2: "Last two cents of payment-per-claim values"
};

const SCOPE_LABELS = {
  within_hcpcs_top200_median: "Top 200 procedure codes (median relationships)"
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
let currentMapStates = [];
let resizeMapTimer = null;
const peerOutlierExpandedByState = {};

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

function getInfoTooltip() {
  let el = document.getElementById("infoTooltip");
  if (!el) {
    el = document.createElement("div");
    el.id = "infoTooltip";
    el.className = "chart-tooltip info-tooltip";
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

function metricLabel(key) {
  if (METRIC_LABELS[key]) return METRIC_LABELS[key];
  return key
    .replace(/^threshold_/, "threshold ")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function metricDescription(key) {
  if (METRIC_DESCRIPTIONS[key]) return METRIC_DESCRIPTIONS[key];
  return "This is a supporting metric used by this signal.";
}

function formatMetricValue(key, value) {
  if (key === "basis") return BASIS_LABELS[String(value)] || String(value);
  if (key === "scope") return SCOPE_LABELS[String(value)] || String(value);
  if (typeof value !== "number") return String(value);

  const percentKeys = new Set([
    "max_abs_dev",
    "smooth_ratio",
    "share_on_5c_grid",
    "share_on_25c_grid",
    "max_cent_bucket_share",
    "threshold_max_abs_dev_hi",
    "threshold_smooth_ratio_lo",
    "threshold_share_on_5c_grid_hi",
    "threshold_share_on_25c_grid_hi",
    "threshold_max_cent_bucket_share_hi"
  ]);
  if (percentKeys.has(key)) return fmtPct(value);

  return fmtN(value);
}

function metricLines(signal) {
  const metrics = signal.metrics || {};
  const entries = Object.entries(metrics).sort(([a], [b]) => {
    const aThreshold = a.startsWith("threshold_") ? 1 : 0;
    const bThreshold = b.startsWith("threshold_") ? 1 : 0;
    if (aThreshold !== bThreshold) return aThreshold - bThreshold;
    return a.localeCompare(b);
  });
  return entries
    .map(
      ([key, value]) => `
        <li class="metric-item">
          <div><strong>${metricLabel(key)}:</strong> <span class="metric-value">${formatMetricValue(key, value)}</span></div>
          <div class="metric-note">${metricDescription(key)}</div>
        </li>`
    )
    .join("");
}

function findingText(signal) {
  const f = signal.failed;
  const name = signal.name;
  if (name === "Reimbursement ratio clustering") {
    return f
      ? "This failed because payment-per-claim spread was higher than the cutoff."
      : "This passed because payment-per-claim spread stayed in range.";
  }
  if (name === "Last digit analysis") {
    return f
      ? "This failed because cents endings were more uneven than expected."
      : "This passed because cents endings looked close to expected.";
  }
  if (name === "Correlation structure") {
    return f
      ? "This failed because at least one key relationship was weaker than the cutoff."
      : "This passed because key relationships were strong enough.";
  }
  if (name === "Temporal noise") {
    return f
      ? "This failed because the timeline looked too smooth."
      : "This passed because the timeline had enough normal ups and downs.";
  }
  if (name === "Entropy") {
    return f
      ? "This failed because value variety was too low."
      : "This passed because value variety was high enough.";
  }
  if (name === "Heaping detection (grid spacing)") {
    return f
      ? "This failed because too many values landed on the same grid steps."
      : "This passed because grid-step clustering stayed in range.";
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
    <strong>What this checks:</strong> ${ref.what}<br>
    <strong>Why it matters:</strong> ${ref.why}<br>
    <strong>Cutoff:</strong> ${ref.thresholds}<br>
    <strong>Result here:</strong> ${findingText(signal)}
  `;
  metricsEl.innerHTML = metricLines(signal);
  boxEl.classList.remove("failed", "passed");
  boxEl.classList.add(signal.failed ? "failed" : "passed");
}

function renderScore(score) {
  document.getElementById("ruleText").textContent = "If 3 or more independent groups fail, flag for deeper review.";
  const familyTotal = Number(score.family_total ?? 0);
  const familyFailCount = Number(score.fail_count ?? 0);
  const rawFailCount = Number(score.raw_fail_count ?? score.fail_count ?? 0);
  const verdictFailCount = Number.isFinite(rawFailCount) ? rawFailCount : familyFailCount;
  if (familyTotal > 0) {
    document.getElementById("failCount").textContent =
      `${familyFailCount} of ${familyTotal} independent groups failed (${rawFailCount} of ${(score.signals || []).length} total checks failed)`;
  } else {
    document.getElementById("failCount").textContent = `${familyFailCount} of ${(score.signals || []).length}`;
  }

  const verdictTextEl = document.getElementById("verdictText");
  const verdictLabelEl = document.getElementById("verdictLabel");
  const verdictRaw = score.verdict || "-";
  let verdictLabel = "PASSED";
  let verdictSummary = "PASSED (fewer than 3 independent groups failed)";
  let verdictClass = "";
  if (verdictFailCount === 3) {
    verdictLabel = "INCONCLUSIVE";
    verdictSummary = "INCONCLUSIVE (exactly 3 independent groups failed)";
    verdictClass = "verdict-inconclusive";
  } else if (verdictFailCount > 3 || verdictRaw === "LIKELY_SYNTHETIC_OR_ALTERED") {
    verdictLabel = "SUSPICIOUS";
    verdictSummary = "SUSPICIOUS (more than 3 independent groups failed)";
    verdictClass = "verdict-alert";
  } else {
    verdictClass = "verdict-pass";
  }

  verdictTextEl.textContent = verdictSummary;
  if (verdictLabelEl) verdictLabelEl.textContent = verdictLabel;
  verdictTextEl.classList.remove("verdict-inconclusive", "verdict-alert", "verdict-pass");
  if (verdictLabelEl) verdictLabelEl.classList.remove("verdict-inconclusive", "verdict-alert", "verdict-pass");
  verdictTextEl.style.color = "";
  if (verdictLabelEl) verdictLabelEl.style.color = "";
  if (verdictClass) {
    verdictTextEl.classList.add(verdictClass);
    if (verdictLabelEl) verdictLabelEl.classList.add(verdictClass);
    if (verdictClass === "verdict-inconclusive") {
      verdictTextEl.style.color = "#ffd36a";
      if (verdictLabelEl) verdictLabelEl.style.color = "#ffd36a";
    } else if (verdictClass === "verdict-alert") {
      verdictTextEl.style.color = "#ff5c5c";
      if (verdictLabelEl) verdictLabelEl.style.color = "#ff5c5c";
    } else if (verdictClass === "verdict-pass") {
      verdictTextEl.style.color = "#64d492";
      if (verdictLabelEl) verdictLabelEl.style.color = "#64d492";
    }
  }
  window.__scoreSignals = score.signals || [];

  const cards = document.getElementById("signalCards");
  cards.innerHTML = "";
  (score.signals || []).forEach((s) => {
    const article = document.createElement("article");
    const targetId = SIGNAL_SECTION_IDS[s.name];
    const displayName = SIGNAL_DISPLAY_NAMES[s.name] || s.name;
    article.className = `card signal-card signal-card-link ${s.failed ? "failed" : "passed"}`;
    article.setAttribute("role", "button");
    article.setAttribute("tabindex", "0");
    article.innerHTML = `
      <div class="signal-head">
        <h3>${displayName}</h3>
        <span class="badge ${s.failed ? "bad" : "good"}">${s.failed ? "FAIL" : "PASS"}</span>
      </div>
    `;
    article.setAttribute("aria-label", `Go to ${displayName} detail`);
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
    ["Rows below the reporting minimum (fewer than 12 claims)", violations.claims_lt_12_rate],
    ["Rows with negative paid amounts", violations.paid_negative_rate],
    ["Rows where people served is greater than claims", violations.benef_gt_claims_rate]
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
    ["People served and claims", c.TOTAL_UNIQUE_BENEFICIARIES?.TOTAL_CLAIMS],
    ["People served and paid amount", c.TOTAL_UNIQUE_BENEFICIARIES?.TOTAL_PAID],
    ["Claims and paid amount", c.TOTAL_CLAIMS?.TOTAL_PAID]
  ];
  const strat = c.within_hcpcs_top200;
  if (strat) {
    pairs.push(["Top 200 procedure codes: people served and claims", strat.median_ben_claims]);
    pairs.push(["Top 200 procedure codes: people served and paid amount", strat.median_ben_paid]);
    pairs.push(["Top 200 procedure codes: claims and paid amount", strat.median_claims_paid]);
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
  const vLabels = ["paid amount", "claims", "people served"];
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
  const toggleBtn = document.getElementById("peerOutlierToggle");
  const summary = document.getElementById("peerOutlierSummary");
  if (!body || !summary || !toggleBtn) return;

  const rows = resolvePeerOutliersForState(activeState);
  const method = peerOutlierBundle.methodology || {};
  const scope = activeState === "ALL" ? "national scope" : `${stateDisplayName(activeState)} scope`;

  if (!toggleBtn.dataset.bound) {
    toggleBtn.addEventListener("click", () => {
      peerOutlierExpandedByState[activeState] = !peerOutlierExpandedByState[activeState];
      renderPeerOutliers();
    });
    toggleBtn.dataset.bound = "1";
  }

  body.innerHTML = "";
  if (!rows.length) {
    summary.textContent = `No providers in ${stateDisplayName(activeState)} had enough data to score reliably.`;
    body.appendChild(row(`<td colspan="9">No providers in ${stateDisplayName(activeState)} had enough data to score reliably.</td>`));
    toggleBtn.style.display = "none";
    return;
  }

  const expanded = !!peerOutlierExpandedByState[activeState];
  const limit = expanded ? 25 : 3;
  const shown = rows.slice(0, Math.min(limit, rows.length));
  summary.textContent =
    `Top providers in ${scope}, ranked by how different they look from peers (${method.peer_cell || "state-level provider peers"}).` +
    ` Showing ${shown.length} of ${Math.min(rows.length, 25)} rows. A higher risk label means bigger differences, not proof of fraud.`;

  if (rows.length > 3) {
    toggleBtn.style.display = "inline-flex";
    toggleBtn.textContent = expanded ? "Show less" : "Show more";
  } else {
    toggleBtn.style.display = "none";
  }

  shown.forEach((r) => {
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
  note.textContent = `Selected area: ${stateDisplayName(activeState)}`;
}

function setupInfoLabelInteractions() {
  const labels = Array.from(document.querySelectorAll(".info-label"));
  if (!labels.length) return;
  const tooltip = getInfoTooltip();
  let pinnedLabel = null;

  const positionTooltip = (label) => {
    const rect = label.getBoundingClientRect();
    const margin = 8;
    tooltip.style.maxWidth = `${Math.max(220, Math.min(320, window.innerWidth - margin * 2))}px`;
    tooltip.style.display = "block";

    const tw = tooltip.offsetWidth;
    const th = tooltip.offsetHeight;
    let left = rect.left;
    let top = rect.bottom + 10;

    if (left + tw > window.innerWidth - margin) left = window.innerWidth - tw - margin;
    if (left < margin) left = margin;
    if (top + th > window.innerHeight - margin) top = rect.top - th - 10;
    if (top < margin) top = margin;

    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };

  const showTooltip = (label) => {
    const text = label.getAttribute("data-help") || "";
    if (!text) return;
    tooltip.textContent = text;
    positionTooltip(label);
  };

  const hideTooltip = () => {
    tooltip.style.display = "none";
    tooltip.textContent = "";
  };

  const closeAll = () => {
    pinnedLabel = null;
    labels.forEach((label) => {
      label.classList.remove("is-open");
      label.setAttribute("aria-expanded", "false");
    });
    hideTooltip();
  };

  labels.forEach((label, idx) => {
    if (!label.dataset.boundInfoLabel) {
      label.setAttribute("role", "button");
      label.setAttribute("tabindex", "0");
      label.setAttribute("aria-expanded", "false");
      label.setAttribute("aria-label", label.textContent.trim() || `Info ${idx + 1}`);
      label.setAttribute("title", label.getAttribute("data-help") || "");

      label.addEventListener("mouseenter", () => {
        if (pinnedLabel && pinnedLabel !== label) return;
        showTooltip(label);
      });

      label.addEventListener("mousemove", () => {
        if (pinnedLabel && pinnedLabel !== label) return;
        positionTooltip(label);
      });

      label.addEventListener("mouseleave", () => {
        if (pinnedLabel === label) return;
        hideTooltip();
      });

      label.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const isOpen = pinnedLabel === label;
        closeAll();
        if (!isOpen) {
          pinnedLabel = label;
          label.classList.add("is-open");
          label.setAttribute("aria-expanded", "true");
          showTooltip(label);
        }
      });

      label.addEventListener("focus", () => {
        if (pinnedLabel && pinnedLabel !== label) return;
        showTooltip(label);
      });

      label.addEventListener("blur", () => {
        if (pinnedLabel === label) return;
        hideTooltip();
      });

      label.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          label.click();
          return;
        }
        if (ev.key === "Escape") {
          closeAll();
          label.blur();
        }
      });

      label.dataset.boundInfoLabel = "1";
    }
  });

  if (!document.body.dataset.boundInfoLabelClose) {
    document.addEventListener("click", () => closeAll());
    document.addEventListener(
      "touchstart",
      (ev) => {
        if (!ev.target || !(ev.target instanceof Element)) return;
        if (!ev.target.closest(".info-label")) closeAll();
      },
      { passive: true }
    );
    window.addEventListener("scroll", () => {
      if (pinnedLabel) positionTooltip(pinnedLabel);
    });
    window.addEventListener("resize", () => {
      if (pinnedLabel) positionTooltip(pinnedLabel);
      else hideTooltip();
    });
    document.body.dataset.boundInfoLabelClose = "1";
  }
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
  currentMapStates = Array.isArray(states) ? states : [];

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
  const viewportWidth = Math.max(window.innerWidth || 0, 320);
  const width = Math.max(320, Math.min(container.clientWidth || viewportWidth, viewportWidth - 16));
  const height = window.innerWidth <= 680 ? 220 : 380;
  const projection = window.d3.geoAlbersUsa().fitExtent(
    [
      [8, 8],
      [width - 8, height - 8]
    ],
    geo
  );
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
    const isMobile = window.innerWidth <= 680;
    const insetW = isMobile ? Math.min(96, Math.max(74, Math.round(width * 0.22))) : 138;
    const insetH = isMobile ? Math.round(insetW * 0.62) : 84;
    const insetPad = isMobile ? 8 : 12;
    const insetX = width - insetW - insetPad;
    const insetY = height - insetH - insetPad;
    const xPad = isMobile ? 7 : 10;
    const yPadTop = isMobile ? 8 : 14;
    const yPadBottom = isMobile ? 13 : 20;
    const prProjection = window.d3.geoMercator().fitExtent(
      [
        [xPad, yPadTop],
        [insetW - xPad, insetH - yPadBottom]
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
      .attr("y", insetH - (isMobile ? 5 : 7))
      .attr("text-anchor", "middle")
      .attr("fill", "#cdd9e4")
      .attr("font-size", isMobile ? "8px" : "10px")
      .attr("font-family", "IBM Plex Mono, monospace")
      .text(isMobile ? "PR" : "Puerto Rico");
  }

  container.appendChild(svg.node());
}

function scheduleMapResize() {
  if (!currentMapStates.length) return;
  if (resizeMapTimer) window.clearTimeout(resizeMapTimer);
  resizeMapTimer = window.setTimeout(async () => {
    await renderUSMap(currentMapStates);
    updateMapActiveState();
  }, 140);
}

loadData().then(async ({ reports, scores, peerOutliers }) => {
  reportBundle = reports || fallbackReportBundle;
  scoreBundle = scores || fallbackScoreBundle;
  peerOutlierBundle = peerOutliers || fallbackPeerOutlierBundle;

  const states = collectAvailableStates();
  const defaultState = reportBundle.default_state || scoreBundle.default_state || "ALL";
  setupStateSelector(states, defaultState);
  await renderUSMap(states);
  setupInfoLabelInteractions();
  window.addEventListener("resize", scheduleMapResize);
  renderActiveState();
});
