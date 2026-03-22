/**
 * Auto CPA Register — Web UI
 */

// ==========================================
// 状态
// ==========================================
const state = {
  task: { status: 'idle', run_id: null, revision: -1 },
  stats: { success: 0, fail: 0, total: 0 },
  ui: {
    autoScroll: true,
    logCount: 0,
    eventSource: null,
    tokenRefreshTimer: null,
    sub2apiRefreshTimer: null,
    sub2apiAccounts: [],
    sub2apiAccountFilter: { status: 'all', keyword: '' },
    sub2apiAccountPager: { page: 1, pageSize: 20, total: 0, filteredTotal: 0, totalPages: 1 },
    selectedSub2ApiAccountIds: new Set(),
    sub2apiAccountsLoading: false,
    sub2apiAccountActionBusy: false,
    // CPA
    cpaAccounts: [],
    cpaAccountFilter: { status: 'all', keyword: '' },
    cpaAccountPager: { page: 1, pageSize: 20, total: 0, filteredTotal: 0, totalPages: 1 },
    selectedCpaAccountNames: new Set(),
    cpaAccountsLoading: false,
    cpaAccountActionBusy: false,
    tokens: [],
  },
};

const $ = id => document.getElementById(id);
const DOM = {};

const STATUS_LABEL_MAP = {
  idle: '空闲', starting: '启动中', running: '运行中',
  stopping: '停止中', stopped: '已停止',
};

const SUB2API_ABNORMAL = new Set(['error', 'disabled']);

// ==========================================
// 初始化
// ==========================================
document.addEventListener('DOMContentLoaded', () => {
  Object.assign(DOM, {
    statusBadge: $('statusBadge'), statusText: $('statusText'), statusDot: $('statusDot'),
    btnStart: $('btnStart'), btnStop: $('btnStop'),
    statSuccess: $('statSuccess'), statFail: $('statFail'), statTotal: $('statTotal'),
    logBody: $('logBody'), logCount: $('logCount'),
    clearLogBtn: $('clearLogBtn'), progressFill: $('progressFill'),
    segmentIndicator: $('segmentIndicator'),
    autoScrollCheck: $('autoScrollCheck'),
    multithreadCheck: $('multithreadCheck'), threadCountInput: $('threadCountInput'),
    totalAccountsInput: $('totalAccountsInput'),
    themeToggleBtn: $('themeToggleBtn'),
    sidebarTokenList: $('sidebarTokenList'),
    tokenRefreshBtn: $('tokenRefreshBtn'),
    // Sub2Api pool
    headerSub2apiChip: $('headerSub2apiChip'),
    headerSub2apiLabel: $('headerSub2apiLabel'),
    headerSub2apiDelta: $('headerSub2apiDelta'),
    headerSub2apiBar: $('headerSub2apiBar'),
    sub2apiPoolTotal: $('sub2apiPoolTotal'), sub2apiPoolNormal: $('sub2apiPoolNormal'),
    sub2apiPoolError: $('sub2apiPoolError'), sub2apiPoolThreshold: $('sub2apiPoolThreshold'),
    sub2apiPoolPercent: $('sub2apiPoolPercent'),
    sub2apiPoolRefreshBtn: $('sub2apiPoolRefreshBtn'),
    sub2apiPoolMaintainBtn: $('sub2apiPoolMaintainBtn'),
    sub2apiPoolMaintainStatus: $('sub2apiPoolMaintainStatus'),
    sub2apiAccountStatusFilter: $('sub2apiAccountStatusFilter'),
    sub2apiAccountKeyword: $('sub2apiAccountKeyword'),
    sub2apiAccountApplyBtn: $('sub2apiAccountApplyBtn'),
    sub2apiAccountResetBtn: $('sub2apiAccountResetBtn'),
    sub2apiAccountSelectAll: $('sub2apiAccountSelectAll'),
    sub2apiAccountSelection: $('sub2apiAccountSelection'),
    sub2apiAccountProbeBtn: $('sub2apiAccountProbeBtn'),
    sub2apiAccountExceptionBtn: $('sub2apiAccountExceptionBtn'),
    sub2apiDuplicateScanBtn: $('sub2apiDuplicateScanBtn'),
    sub2apiDuplicateCleanBtn: $('sub2apiDuplicateCleanBtn'),
    sub2apiAccountDeleteBtn: $('sub2apiAccountDeleteBtn'),
    sub2apiAccountList: $('sub2apiAccountList'),
    sub2apiAccountActionStatus: $('sub2apiAccountActionStatus'),
    sub2apiAccountPrevBtn: $('sub2apiAccountPrevBtn'),
    sub2apiAccountNextBtn: $('sub2apiAccountNextBtn'),
    sub2apiAccountPageInfo: $('sub2apiAccountPageInfo'),
    sub2apiAccountPageSize: $('sub2apiAccountPageSize'),
    // Config
    duckmailApiBase: $('duckmailApiBase'), duckmailBearer: $('duckmailBearer'),
    duckmailSaveBtn: $('duckmailSaveBtn'), duckmailStatus: $('duckmailStatus'),
    proxyListUrl: $('proxyListUrl'), proxyListBearer: $('proxyListBearer'),
    proxyInput: $('proxyInput'), stableProxyInput: $('stableProxyInput'),
    proxyValidateTimeout: $('proxyValidateTimeout'), proxyValidateWorkers: $('proxyValidateWorkers'),
    proxyValidateEnabled: $('proxyValidateEnabled'), preferStableProxy: $('preferStableProxy'),
    proxySaveBtn: $('proxySaveBtn'), proxyStatus: $('proxyStatus'),
    sub2apiBaseUrl: $('sub2apiBaseUrl'), sub2apiMinCandidates: $('sub2apiMinCandidates'),
    sub2apiEmail: $('sub2apiEmail'), sub2apiPassword: $('sub2apiPassword'),
    sub2apiGroupIds: $('sub2apiGroupIds'), sub2apiProxyId: $('sub2apiProxyId'),
    sub2apiProxyName: $('sub2apiProxyName'),
    autoUploadSub2api: $('autoUploadSub2api'), sub2apiAutoAssignProxy: $('sub2apiAutoAssignProxy'),
    sub2apiTestBtn: $('sub2apiTestBtn'), sub2apiSaveBtn: $('sub2apiSaveBtn'),
    sub2apiConfigStatus: $('sub2apiConfigStatus'),
    // CPA pool
    headerCpaChip: $('headerCpaChip'),
    headerCpaLabel: $('headerCpaLabel'),
    headerCpaDelta: $('headerCpaDelta'),
    headerCpaBar: $('headerCpaBar'),
    cpaPoolTotal: $('cpaPoolTotal'), cpaPoolNormal: $('cpaPoolNormal'),
    cpaPoolError: $('cpaPoolError'), cpaPoolThreshold: $('cpaPoolThreshold'),
    cpaPoolPercent: $('cpaPoolPercent'),
    cpaPoolRefreshBtn: $('cpaPoolRefreshBtn'),
    cpaPoolRefreshStatus: $('cpaPoolRefreshStatus'),
    cpaAccountStatusFilter: $('cpaAccountStatusFilter'),
    cpaAccountKeyword: $('cpaAccountKeyword'),
    cpaAccountApplyBtn: $('cpaAccountApplyBtn'),
    cpaAccountResetBtn: $('cpaAccountResetBtn'),
    cpaAccountSelectAll: $('cpaAccountSelectAll'),
    cpaAccountSelection: $('cpaAccountSelection'),
    cpaAccountDeleteBtn: $('cpaAccountDeleteBtn'),
    cpaAccountList: $('cpaAccountList'),
    cpaAccountActionStatus: $('cpaAccountActionStatus'),
    cpaAccountPrevBtn: $('cpaAccountPrevBtn'),
    cpaAccountNextBtn: $('cpaAccountNextBtn'),
    cpaAccountPageInfo: $('cpaAccountPageInfo'),
    cpaAccountPageSize: $('cpaAccountPageSize'),
    // CPA config
    cpaBaseUrl: $('cpaBaseUrl'), cpaMinCandidates: $('cpaMinCandidates'),
    cpaManagementKey: $('cpaManagementKey'), autoUploadCpa: $('autoUploadCpa'),
    cpaTestBtn: $('cpaTestBtn'), cpaSaveBtn: $('cpaSaveBtn'),
    cpaConfigStatus: $('cpaConfigStatus'),
  });

  connectSSE();
  loadConfig();
  pollSub2ApiPoolStatus();
  loadSub2ApiAccounts();
  pollCpaPoolStatus();
  loadCpaAccounts();
  loadTokens();
  initThemeSwitch();
  initCollapsibles();
  initTabs();

  DOM.btnStart.addEventListener('click', startTask);
  DOM.btnStop.addEventListener('click', stopTask);
  DOM.clearLogBtn.addEventListener('click', clearLog);
  if (DOM.tokenRefreshBtn) DOM.tokenRefreshBtn.addEventListener('click', loadTokens);

  if (DOM.sub2apiPoolRefreshBtn) DOM.sub2apiPoolRefreshBtn.addEventListener('click', () => { pollSub2ApiPoolStatus(); loadSub2ApiAccounts(); });
  if (DOM.sub2apiPoolMaintainBtn) DOM.sub2apiPoolMaintainBtn.addEventListener('click', triggerSub2ApiMaintenance);
  if (DOM.sub2apiAccountApplyBtn) DOM.sub2apiAccountApplyBtn.addEventListener('click', applySub2ApiAccountFilter);
  if (DOM.sub2apiAccountResetBtn) DOM.sub2apiAccountResetBtn.addEventListener('click', resetSub2ApiAccountFilter);
  if (DOM.sub2apiAccountKeyword) DOM.sub2apiAccountKeyword.addEventListener('keydown', e => { if (e.key === 'Enter') applySub2ApiAccountFilter(); });
  if (DOM.sub2apiAccountPrevBtn) DOM.sub2apiAccountPrevBtn.addEventListener('click', () => changeSub2ApiAccountPage(-1));
  if (DOM.sub2apiAccountNextBtn) DOM.sub2apiAccountNextBtn.addEventListener('click', () => changeSub2ApiAccountPage(1));
  if (DOM.sub2apiAccountPageSize) DOM.sub2apiAccountPageSize.addEventListener('change', changeSub2ApiAccountPageSize);
  if (DOM.sub2apiAccountSelectAll) DOM.sub2apiAccountSelectAll.addEventListener('change', toggleSelectAllSub2ApiAccounts);
  if (DOM.sub2apiAccountProbeBtn) DOM.sub2apiAccountProbeBtn.addEventListener('click', triggerSelectedSub2ApiProbe);
  if (DOM.sub2apiAccountExceptionBtn) DOM.sub2apiAccountExceptionBtn.addEventListener('click', triggerSub2ApiExceptionHandling);
  if (DOM.sub2apiDuplicateScanBtn) DOM.sub2apiDuplicateScanBtn.addEventListener('click', previewSub2ApiDuplicates);
  if (DOM.sub2apiDuplicateCleanBtn) DOM.sub2apiDuplicateCleanBtn.addEventListener('click', cleanupSub2ApiDuplicates);
  if (DOM.sub2apiAccountDeleteBtn) DOM.sub2apiAccountDeleteBtn.addEventListener('click', triggerSelectedSub2ApiDelete);

  if (DOM.sub2apiAccountList) {
    DOM.sub2apiAccountList.addEventListener('click', async e => {
      const probeBtn = e.target.closest('.sub2api-account-probe-btn');
      if (probeBtn) { await runSub2ApiAccountProbe([parseInt(probeBtn.dataset.accountId, 10)], `账号 ${probeBtn.dataset.accountId}`); return; }
      const deleteBtn = e.target.closest('.sub2api-account-delete-btn');
      if (deleteBtn) await runSub2ApiAccountDelete([parseInt(deleteBtn.dataset.accountId, 10)], decodeURIComponent(deleteBtn.dataset.email || ''));
    });
    DOM.sub2apiAccountList.addEventListener('change', e => {
      const cb = e.target.closest('.sub2api-account-check');
      if (!cb) return;
      const id = parseInt(cb.dataset.accountId, 10);
      if (cb.checked) state.ui.selectedSub2ApiAccountIds.add(id);
      else state.ui.selectedSub2ApiAccountIds.delete(id);
      const row = cb.closest('.sub2api-account-item');
      if (row) row.classList.toggle('selected', cb.checked);
      refreshSub2ApiSelectionState();
    });
  }

  DOM.logBody.addEventListener('scroll', () => {
    const el = DOM.logBody;
    state.ui.autoScroll = el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
    if (DOM.autoScrollCheck) DOM.autoScrollCheck.checked = state.ui.autoScroll;
  });
  if (DOM.autoScrollCheck) {
    DOM.autoScrollCheck.addEventListener('change', () => {
      state.ui.autoScroll = DOM.autoScrollCheck.checked;
      if (state.ui.autoScroll) DOM.logBody.scrollTop = DOM.logBody.scrollHeight;
    });
  }

  if (DOM.duckmailSaveBtn) DOM.duckmailSaveBtn.addEventListener('click', saveDuckmailConfig);
  if (DOM.proxySaveBtn) DOM.proxySaveBtn.addEventListener('click', saveProxyConfig);
  if (DOM.sub2apiTestBtn) DOM.sub2apiTestBtn.addEventListener('click', testSub2ApiPoolConnection);
  if (DOM.sub2apiSaveBtn) DOM.sub2apiSaveBtn.addEventListener('click', saveSub2ApiConfig);

  // CPA event listeners
  if (DOM.cpaPoolRefreshBtn) DOM.cpaPoolRefreshBtn.addEventListener('click', () => { pollCpaPoolStatus(); loadCpaAccounts(); });
  if (DOM.cpaAccountApplyBtn) DOM.cpaAccountApplyBtn.addEventListener('click', applyCpaAccountFilter);
  if (DOM.cpaAccountResetBtn) DOM.cpaAccountResetBtn.addEventListener('click', resetCpaAccountFilter);
  if (DOM.cpaAccountKeyword) DOM.cpaAccountKeyword.addEventListener('keydown', e => { if (e.key === 'Enter') applyCpaAccountFilter(); });
  if (DOM.cpaAccountPrevBtn) DOM.cpaAccountPrevBtn.addEventListener('click', () => changeCpaAccountPage(-1));
  if (DOM.cpaAccountNextBtn) DOM.cpaAccountNextBtn.addEventListener('click', () => changeCpaAccountPage(1));
  if (DOM.cpaAccountPageSize) DOM.cpaAccountPageSize.addEventListener('change', changeCpaAccountPageSize);
  if (DOM.cpaAccountSelectAll) DOM.cpaAccountSelectAll.addEventListener('change', toggleSelectAllCpaAccounts);
  if (DOM.cpaAccountDeleteBtn) DOM.cpaAccountDeleteBtn.addEventListener('click', triggerSelectedCpaDelete);

  if (DOM.cpaAccountList) {
    DOM.cpaAccountList.addEventListener('click', async e => {
      const deleteBtn = e.target.closest('.cpa-account-delete-btn');
      if (deleteBtn) await runCpaAccountDelete([deleteBtn.dataset.fileName], decodeURIComponent(deleteBtn.dataset.email || ''));
    });
    DOM.cpaAccountList.addEventListener('change', e => {
      const cb = e.target.closest('.cpa-account-check');
      if (!cb) return;
      const name = cb.dataset.fileName;
      if (cb.checked) state.ui.selectedCpaAccountNames.add(name);
      else state.ui.selectedCpaAccountNames.delete(name);
      const row = cb.closest('.cpa-account-item');
      if (row) row.classList.toggle('selected', cb.checked);
      refreshCpaSelectionState();
    });
  }

  if (DOM.cpaTestBtn) DOM.cpaTestBtn.addEventListener('click', testCpaConnection);
  if (DOM.cpaSaveBtn) DOM.cpaSaveBtn.addEventListener('click', saveCpaConfig);

  // Remote panel tab switching
  document.querySelectorAll('.remote-tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.remote-tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const targetId = btn.dataset.panel;
      document.querySelectorAll('.data-panel-section').forEach(sec => sec.classList.remove('active'));
      const target = document.getElementById(targetId);
      if (target) target.classList.add('active');
    });
  });

  setInterval(pollSub2ApiPoolStatus, 30000);
  setInterval(() => loadSub2ApiAccounts({ silent: true }), 60000);
  setInterval(pollCpaPoolStatus, 30000);
  setInterval(() => loadCpaAccounts({ silent: true }), 60000);
  setInterval(loadTokens, 60000);
});

// ==========================================
// Tab 导航
// ==========================================
function initTabs() {
  document.querySelectorAll('.tab-btn').forEach((btn, index) => {
    btn.addEventListener('click', () => switchMainTab(btn.dataset.tab));
  });
  switchMainTab('tabDashboard');
}

function switchMainTab(tabId) {
  const next = tabId === 'tabConfig' ? 'tabConfig' : 'tabDashboard';
  document.querySelectorAll('.tab-btn').forEach((btn, i) => {
    const active = btn.dataset.tab === next;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-selected', active ? 'true' : 'false');
    if (active && DOM.segmentIndicator) DOM.segmentIndicator.setAttribute('data-active', String(i));
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === next));
}

function initCollapsibles() {
  document.querySelectorAll('.collapsible-trigger').forEach(trigger => {
    trigger.addEventListener('click', () => {
      const section = trigger.closest('.collapsible');
      if (!section) return;
      const body = section.querySelector('.collapsible-body');
      if (!body) return;
      const isOpen = section.classList.contains('open');
      section.classList.toggle('open', !isOpen);
      body.style.display = isOpen ? 'none' : 'block';
    });
  });
}

// ==========================================
// SSE
// ==========================================
function connectSSE() {
  if (state.ui.eventSource) state.ui.eventSource.close();
  const es = new EventSource('/api/logs');
  state.ui.eventSource = es;

  const handle = (sourceType, raw) => {
    try {
      const payload = raw?.data ? JSON.parse(raw.data) : {};
      if (!payload.type && sourceType !== 'message') payload.type = sourceType;
      applySseEvent(payload);
    } catch {}
  };

  ['connected', 'snapshot', 'task.updated', 'stats.updated', 'log.appended'].forEach(name => {
    es.addEventListener(name, e => handle(name, e));
  });
  es.onmessage = e => handle('message', e);
  es.onerror = () => setTimeout(connectSSE, 3000);
}

function applySseEvent(event) {
  if (!event || typeof event !== 'object') return;
  const type = String(event.type || '');

  if (type === 'connected') {
    appendLog({ ts: event.ts || '', level: 'connected', message: '实时事件已连接' });
    if (event.snapshot) applySnapshot(event.snapshot);
    return;
  }
  if (type === 'log.appended') {
    const log = event.log && typeof event.log === 'object' ? event.log : event;
    appendLog(log);
    const msg = String(log.message || '');
    if (msg.includes('Token 已保存') || msg.includes('[Sub2Api] 上传') || msg.includes('[CPA] 上传')) {
      scheduleTokenRefresh();
    }
    if (msg.includes('[Sub2Api] 上传')) {
      scheduleSub2ApiRefresh();
    }
    return;
  }
  if (type === 'task.updated') {
    if (event.task) state.task = { ...state.task, ...event.task };
    if (event.stats) { state.stats = { ...state.stats, ...event.stats }; syncStatsUI(); }
    syncTaskChrome();
    return;
  }
  if (type === 'snapshot' || (event.task && event.stats)) {
    applySnapshot(event.snapshot || event);
  }
}

function applySnapshot(snap) {
  if (!snap) return;
  if (snap.task) state.task = { ...state.task, ...snap.task };
  if (snap.stats) { state.stats = { ...state.stats, ...snap.stats }; }
  syncTaskChrome();
  syncStatsUI();
}

// ==========================================
// Log rendering
// ==========================================
const LEVEL_ICON = { info: '›', success: '✓', error: '✗', warn: '⚠', connected: '⟳' };

function appendLog(event) {
  const { ts, level, message, step } = event;
  state.ui.logCount++;
  const entry = document.createElement('div');
  entry.className = 'log-entry';
  entry.innerHTML = `
    <span class="log-ts">${escapeHtml(ts || '')}</span>
    <span class="log-icon">${LEVEL_ICON[level] || '·'}</span>
    <span class="log-msg ${escapeHtml(level || 'info')}">${escapeHtml(message || '')}</span>
    ${step ? `<span class="log-step">${escapeHtml(step)}</span>` : ''}
  `;
  DOM.logBody.appendChild(entry);
  DOM.logCount.textContent = state.ui.logCount;
  if (state.ui.autoScroll) DOM.logBody.scrollTop = DOM.logBody.scrollHeight;
  if (DOM.logBody.children.length > 2000) DOM.logBody.firstElementChild.remove();
}

function clearLog() {
  DOM.logBody.innerHTML = '';
  state.ui.logCount = 0;
  DOM.logCount.textContent = '0';
}

// ==========================================
// Task control
// ==========================================
function getWorkerCount() {
  if (!DOM.multithreadCheck?.checked) return 1;
  return Math.max(1, parseInt(DOM.threadCountInput?.value || '1', 10) || 1);
}

function getTotalAccounts() {
  return Math.max(1, parseInt(DOM.totalAccountsInput?.value || '3', 10) || 3);
}

async function startTask() {
  try {
    state.stats = { success: 0, fail: 0, total: 0 };
    syncStatsUI();
    const res = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ worker_count: getWorkerCount(), total_accounts: getTotalAccounts() }),
    });
    const data = await res.json();
    if (!res.ok) { showToast(data.detail || '启动失败', 'error'); return; }
    applySnapshot(data);
    showToast(`注册任务已启动 (${getTotalAccounts()} 个账号, ${getWorkerCount()} 线程)`, 'success');
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  }
}

async function stopTask() {
  try {
    const res = await fetch('/api/stop', { method: 'POST' });
    const data = await res.json();
    if (!res.ok) { showToast(data.detail || '停止失败', 'error'); return; }
    applySnapshot(data);
    showToast('停止指令已发送', 'info');
  } catch (e) {
    showToast('停止失败: ' + e.message, 'error');
  }
}

function syncTaskChrome() {
  const status = state.task.status || 'idle';
  DOM.statusBadge.className = `status-badge ${status}`;
  DOM.statusText.textContent = STATUS_LABEL_MAP[status] || status;

  const isRunning = status === 'running';
  const isStopping = status === 'stopping';
  const canStop = isRunning;
  DOM.btnStart.disabled = isRunning || isStopping;
  DOM.btnStop.disabled = !canStop;
  DOM.progressFill.className = isRunning ? 'progress-fill running' : (isStopping ? 'progress-fill stopping' : 'progress-fill');
}

function syncStatsUI() {
  if (DOM.statSuccess) DOM.statSuccess.textContent = state.stats.success;
  if (DOM.statFail) DOM.statFail.textContent = state.stats.fail;
  if (DOM.statTotal) DOM.statTotal.textContent = state.stats.total;
}

// ==========================================
// Tokens (sidebar)
// ==========================================
async function loadTokens() {
  try {
    const res = await fetch('/api/tokens');
    const data = await res.json();
    state.ui.tokens = data.tokens || [];
    renderSidebarTokens();
  } catch {}
}

function renderSidebarTokens() {
  const el = DOM.sidebarTokenList;
  if (!el) return;
  const tokens = state.ui.tokens || [];
  if (!tokens.length) {
    el.innerHTML = '<div style="color:var(--text-muted);padding:8px 0;font-size:11px;">暂无 Token</div>';
    return;
  }
  const getSyncMeta = (token, platform) => {
    const syncStatus = token && typeof token.sync_status === 'object' ? token.sync_status : {};
    const item = syncStatus && typeof syncStatus[platform] === 'object' ? syncStatus[platform] : null;
    const uploaded = Array.isArray(token?.uploaded_platforms)
      ? token.uploaded_platforms.map(v => String(v).trim().toLowerCase())
      : [];
    if (item) {
      return {
        ok: !!item.ok,
        detail: item.detail || '',
        updatedAt: item.updated_at || '',
      };
    }
    if (uploaded.includes(platform)) {
      return { ok: true, detail: '', updatedAt: '' };
    }
    return null;
  };
  const buildSyncBadge = (platform, label, meta) => {
    if (!meta) return '';
    const title = [label, meta.ok ? '已同步' : '同步失败', formatTime(meta.updatedAt), meta.detail]
      .filter(Boolean)
      .join(' | ');
    const styles = meta.ok
      ? 'color:var(--accent-green);background:var(--accent-green-dim);'
      : 'color:var(--danger);background:rgba(255,107,107,.14);';
    const text = meta.ok ? `${label}已同步` : `${label}失败`;
    return `<span title="${escapeHtml(title)}" style="font-size:9px;${styles}padding:1px 5px;border-radius:4px;margin-left:4px;">${escapeHtml(text)}</span>`;
  };
  el.innerHTML = tokens.slice(0, 50).map(t => {
    const badges = [
      buildSyncBadge('sub2api', 'Sub2Api', getSyncMeta(t, 'sub2api')),
      buildSyncBadge('cpa', 'CPA', getSyncMeta(t, 'cpa')),
    ].join('');
    return `<div style="padding:4px 0;border-bottom:1px solid var(--separator);display:flex;align-items:center;justify-content:space-between;gap:4px;">
      <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1;" title="${escapeHtml(t.email)}">${escapeHtml(t.email || t.filename)}</span>
      ${badges}
    </div>`;
  }).join('');
}

function scheduleTokenRefresh(delay = 500) {
  if (state.ui.tokenRefreshTimer) clearTimeout(state.ui.tokenRefreshTimer);
  state.ui.tokenRefreshTimer = setTimeout(() => {
    state.ui.tokenRefreshTimer = null;
    loadTokens();
  }, delay);
}

function scheduleSub2ApiRefresh(delay = 900) {
  if (state.ui.sub2apiRefreshTimer) clearTimeout(state.ui.sub2apiRefreshTimer);
  state.ui.sub2apiRefreshTimer = setTimeout(() => {
    state.ui.sub2apiRefreshTimer = null;
    pollSub2ApiPoolStatus();
    loadSub2ApiAccounts({ silent: true });
  }, delay);
}

// ==========================================
// Sub2Api Pool Status
// ==========================================
async function pollSub2ApiPoolStatus() {
  try {
    const res = await fetch('/api/sub2api/pool/status');
    const data = await res.json();

    if (!data.configured) {
      ['sub2apiPoolTotal', 'sub2apiPoolNormal', 'sub2apiPoolError', 'sub2apiPoolThreshold', 'sub2apiPoolPercent'].forEach(id => {
        const el = $(id); if (el) el.textContent = '--';
      });
      updateHeaderSub2Api(null);
      return;
    }

    if (data.error) {
      if (DOM.sub2apiPoolTotal) DOM.sub2apiPoolTotal.textContent = '--';
      if (DOM.sub2apiPoolNormal) DOM.sub2apiPoolNormal.textContent = '--';
      if (DOM.sub2apiPoolError) {
        DOM.sub2apiPoolError.textContent = 'ERR';
        DOM.sub2apiPoolError.className = 'stat-value red';
      }
      if (DOM.sub2apiPoolThreshold) DOM.sub2apiPoolThreshold.textContent = data.threshold || '--';
      if (DOM.sub2apiPoolPercent) {
        DOM.sub2apiPoolPercent.textContent = '失败';
        DOM.sub2apiPoolPercent.className = 'stat-value red';
      }
      if (DOM.sub2apiPoolMaintainStatus) DOM.sub2apiPoolMaintainStatus.textContent = `连接异常: ${data.error}`;
      updateHeaderSub2Api({ normal: 0, threshold: data.threshold || 0, fillPct: 0, error: 1 });
      return;
    }

    const normal = data.candidates || 0;
    const error = data.error_count || 0;
    const total = data.total || 0;
    const threshold = data.threshold || 0;
    const fillPct = threshold > 0 ? Math.round(normal / threshold * 100) : 100;

    if (DOM.sub2apiPoolTotal) DOM.sub2apiPoolTotal.textContent = total;
    if (DOM.sub2apiPoolNormal) DOM.sub2apiPoolNormal.textContent = normal;
    if (DOM.sub2apiPoolError) {
      DOM.sub2apiPoolError.textContent = error;
      DOM.sub2apiPoolError.className = `stat-value ${error > 0 ? 'red' : 'green'}`;
    }
    if (DOM.sub2apiPoolThreshold) DOM.sub2apiPoolThreshold.textContent = threshold;
    if (DOM.sub2apiPoolPercent) {
      DOM.sub2apiPoolPercent.textContent = fillPct + '%';
      DOM.sub2apiPoolPercent.className = `stat-value ${fillPct >= 100 ? 'green' : fillPct >= 80 ? '' : 'red'}`;
    }
    if (DOM.sub2apiPoolMaintainStatus && DOM.sub2apiPoolMaintainStatus.textContent.startsWith('连接异常:')) {
      DOM.sub2apiPoolMaintainStatus.textContent = '';
    }
    updateHeaderSub2Api({ normal, threshold, fillPct, error });
  } catch (e) {
    if (DOM.sub2apiPoolMaintainStatus) DOM.sub2apiPoolMaintainStatus.textContent = '请求失败: ' + e.message;
  }
}

function updateHeaderSub2Api(data) {
  if (!data) {
    if (DOM.headerSub2apiLabel) DOM.headerSub2apiLabel.textContent = '-- / --';
    if (DOM.headerSub2apiDelta) DOM.headerSub2apiDelta.textContent = '--';
    if (DOM.headerSub2apiBar) DOM.headerSub2apiBar.style.width = '0%';
    setChipStatus(DOM.headerSub2apiChip, 'idle');
    return;
  }
  const { normal, threshold, fillPct, error } = data;
  const st = error > 0 ? 'danger' : (fillPct > 110 ? 'over' : fillPct >= 100 ? 'ok' : fillPct >= 80 ? 'warn' : 'danger');
  if (DOM.headerSub2apiLabel) DOM.headerSub2apiLabel.textContent = `${normal} / ${threshold}`;
  if (DOM.headerSub2apiDelta) {
    const delta = Math.round(fillPct - 100);
    DOM.headerSub2apiDelta.textContent = delta === 0 ? '0%' : `${delta > 0 ? '+' : ''}${delta}%`;
    DOM.headerSub2apiDelta.className = `pool-chip-delta ${st === 'idle' ? '' : st}`.trim();
  }
  if (DOM.headerSub2apiBar) {
    DOM.headerSub2apiBar.style.width = Math.min(100, fillPct) + '%';
    DOM.headerSub2apiBar.className = `pool-chip-fill ${st === 'idle' ? '' : st}`.trim();
  }
  setChipStatus(DOM.headerSub2apiChip, st);
}

function setChipStatus(chip, st) {
  if (!chip) return;
  chip.classList.remove('status-idle', 'status-warn', 'status-danger', 'status-ok', 'status-over');
  chip.classList.add(`status-${st}`);
}

async function triggerSub2ApiMaintenance() {
  DOM.sub2apiPoolMaintainBtn.disabled = true;
  DOM.sub2apiPoolMaintainBtn.textContent = '维护中...';
  DOM.sub2apiPoolMaintainStatus.textContent = '正在维护...';
  try {
    const res = await fetch('/api/sub2api/pool/maintain', { method: 'POST' });
    const data = await res.json();
    if (res.ok) {
      const sec = (data.duration_ms / 1000).toFixed(2);
      const msg = `维护完成: 异常 ${data.error_count || 0}, 刷新恢复 ${data.refreshed || 0}, 重复组 ${data.duplicate_groups || 0}, 删除 ${data.deleted_ok || 0}, ${sec}s`;
      DOM.sub2apiPoolMaintainStatus.textContent = msg;
      showToast(msg, 'success');
      pollSub2ApiPoolStatus();
      loadSub2ApiAccounts({ silent: true });
    } else {
      DOM.sub2apiPoolMaintainStatus.textContent = data.detail || '维护失败';
      showToast(data.detail || '维护失败', 'error');
    }
  } catch (e) {
    DOM.sub2apiPoolMaintainStatus.textContent = '请求失败: ' + e.message;
    showToast('维护失败', 'error');
  } finally {
    DOM.sub2apiPoolMaintainBtn.disabled = false;
    DOM.sub2apiPoolMaintainBtn.textContent = '维护';
  }
}

async function testSub2ApiPoolConnection() {
  DOM.sub2apiTestBtn.disabled = true;
  DOM.sub2apiConfigStatus.textContent = '测试中...';
  try {
    const res = await fetch('/api/sub2api/pool/check', { method: 'POST' });
    const data = await res.json();
    DOM.sub2apiConfigStatus.textContent = data.message || (data.ok ? '连接成功' : '连接失败');
    if (data.ok) {
      pollSub2ApiPoolStatus();
      loadSub2ApiAccounts({ silent: true });
    }
    showToast(data.ok ? 'Sub2Api 连接成功' : 'Sub2Api 连接失败', data.ok ? 'success' : 'error');
  } catch (e) {
    DOM.sub2apiConfigStatus.textContent = '请求失败: ' + e.message;
  } finally {
    DOM.sub2apiTestBtn.disabled = false;
  }
}

// ==========================================
// Sub2Api Accounts
// ==========================================
function applySub2ApiAccountFilter() {
  state.ui.sub2apiAccountFilter.status = DOM.sub2apiAccountStatusFilter?.value || 'all';
  state.ui.sub2apiAccountFilter.keyword = DOM.sub2apiAccountKeyword?.value.trim() || '';
  state.ui.sub2apiAccountPager.page = 1;
  loadSub2ApiAccounts();
}

function resetSub2ApiAccountFilter() {
  state.ui.sub2apiAccountFilter = { status: 'all', keyword: '' };
  if (DOM.sub2apiAccountStatusFilter) DOM.sub2apiAccountStatusFilter.value = 'all';
  if (DOM.sub2apiAccountKeyword) DOM.sub2apiAccountKeyword.value = '';
  state.ui.sub2apiAccountPager.page = 1;
  loadSub2ApiAccounts();
}

async function loadSub2ApiAccounts({ silent = false } = {}) {
  if (!DOM.sub2apiAccountList || state.ui.sub2apiAccountsLoading) return;
  state.ui.sub2apiAccountsLoading = true;
  if (!silent && DOM.sub2apiAccountActionStatus && !state.ui.sub2apiAccountActionBusy) {
    DOM.sub2apiAccountActionStatus.textContent = '正在加载...';
  }
  try {
    const p = state.ui.sub2apiAccountPager;
    const f = state.ui.sub2apiAccountFilter;
    const params = new URLSearchParams({
      page: String(p.page || 1),
      page_size: String(p.pageSize || 20),
      status: String(f.status || 'all'),
      keyword: String(f.keyword || ''),
    });
    const res = await fetch(`/api/sub2api/accounts?${params}`);
    const data = await res.json();
    if (!data.configured) {
      state.ui.sub2apiAccounts = [];
      renderSub2ApiAccountList('请先完成 Sub2Api 平台配置');
      if (DOM.sub2apiAccountActionStatus && !state.ui.sub2apiAccountActionBusy)
        DOM.sub2apiAccountActionStatus.textContent = 'Sub2Api 未配置';
      return;
    }
    if (data.error) {
      state.ui.sub2apiAccounts = [];
      state.ui.sub2apiAccountPager.total = 0;
      state.ui.sub2apiAccountPager.filteredTotal = 0;
      state.ui.sub2apiAccountPager.totalPages = 1;
      renderSub2ApiAccountList(`账号列表加载失败: ${data.error}`);
      if (DOM.sub2apiAccountActionStatus && !state.ui.sub2apiAccountActionBusy)
        DOM.sub2apiAccountActionStatus.textContent = '加载失败: ' + data.error;
      return;
    }
    state.ui.sub2apiAccounts = Array.isArray(data.items) ? data.items : [];
    state.ui.sub2apiAccountPager.page = parseInt(data.page, 10) || 1;
    state.ui.sub2apiAccountPager.pageSize = parseInt(data.page_size, 10) || 20;
    state.ui.sub2apiAccountPager.total = parseInt(data.total, 10) || 0;
    state.ui.sub2apiAccountPager.filteredTotal = parseInt(data.filtered_total, 10) || 0;
    state.ui.sub2apiAccountPager.totalPages = parseInt(data.total_pages, 10) || 1;
    renderSub2ApiAccountList();
    if (!silent && DOM.sub2apiAccountActionStatus && !state.ui.sub2apiAccountActionBusy) {
      const pp = state.ui.sub2apiAccountPager;
      DOM.sub2apiAccountActionStatus.textContent = `第 ${pp.page}/${pp.totalPages} 页，共 ${pp.filteredTotal} 个账号`;
    }
  } catch (e) {
    renderSub2ApiAccountList('账号列表加载失败');
    if (DOM.sub2apiAccountActionStatus && !state.ui.sub2apiAccountActionBusy)
      DOM.sub2apiAccountActionStatus.textContent = '加载失败: ' + e.message;
  } finally {
    state.ui.sub2apiAccountsLoading = false;
    refreshSub2ApiSelectionState();
  }
}

function renderSub2ApiAccountList(emptyMessage = '') {
  const accounts = state.ui.sub2apiAccounts || [];
  const pager = state.ui.sub2apiAccountPager;
  if (!DOM.sub2apiAccountList) return;
  if (!accounts.length) {
    const msg = emptyMessage || '暂无账号';
    DOM.sub2apiAccountList.innerHTML = `<div class="empty-state"><div class="empty-icon">□</div><span>${escapeHtml(msg)}</span></div>`;
  } else {
    DOM.sub2apiAccountList.innerHTML = accounts.map(renderSub2ApiAccountItem).join('');
  }
  updateSub2ApiPagerUI();
  refreshSub2ApiSelectionState();
}

function renderSub2ApiAccountItem(account) {
  const id = Number(account.id || 0);
  const email = account.email || account.name || `账号 ${id}`;
  const status = String(account.status || 'unknown').toLowerCase();
  const isAbnormal = SUB2API_ABNORMAL.has(status);
  const selected = state.ui.selectedSub2ApiAccountIds.has(id);
  const statusLabel = { error: '异常', disabled: '禁用', normal: '正常', active: '正常', unknown: '未知' }[status] || status;
  const statusClass = status === 'disabled' ? 'warn' : (isAbnormal ? 'danger' : 'ok');
  return `
    <div class="token-item sub2api-account-item${selected ? ' selected' : ''}" id="sub2api-account-${id}">
      <label class="account-check-wrap">
        <input type="checkbox" class="sub2api-account-check" data-account-id="${id}" ${selected ? 'checked' : ''} />
      </label>
      <div class="token-info">
        <div class="token-email" title="${escapeHtml(email)}">
          <span class="token-email-text">${escapeHtml(email)}</span>
          <span class="account-status-badge ${statusClass}">${escapeHtml(statusLabel)}</span>
        </div>
        <div class="token-meta">ID: ${id} · ${escapeHtml(formatTime(account.updated_at))}</div>
      </div>
      <div class="token-actions">
        <button class="btn btn-ghost btn-sm sub2api-account-probe-btn" data-account-id="${id}">测活</button>
        <button class="btn btn-danger btn-sm sub2api-account-delete-btn" data-account-id="${id}" data-email="${encodeURIComponent(email)}">删除</button>
      </div>
    </div>`;
}

function updateSub2ApiPagerUI() {
  const { page, totalPages, pageSize } = state.ui.sub2apiAccountPager;
  if (DOM.sub2apiAccountPageInfo)
    DOM.sub2apiAccountPageInfo.textContent = `第 ${page}/${totalPages} 页 · 每页 ${pageSize} 条`;
  if (DOM.sub2apiAccountPageSize && String(DOM.sub2apiAccountPageSize.value) !== String(pageSize))
    DOM.sub2apiAccountPageSize.value = String(pageSize);
  if (DOM.sub2apiAccountPrevBtn) DOM.sub2apiAccountPrevBtn.disabled = state.ui.sub2apiAccountActionBusy || page <= 1;
  if (DOM.sub2apiAccountNextBtn) DOM.sub2apiAccountNextBtn.disabled = state.ui.sub2apiAccountActionBusy || page >= totalPages;
}

function changeSub2ApiAccountPage(delta) {
  const next = (state.ui.sub2apiAccountPager.page || 1) + delta;
  if (next < 1 || next > state.ui.sub2apiAccountPager.totalPages) return;
  state.ui.sub2apiAccountPager.page = next;
  loadSub2ApiAccounts();
}

function changeSub2ApiAccountPageSize() {
  state.ui.sub2apiAccountPager.pageSize = parseInt(DOM.sub2apiAccountPageSize?.value || '20', 10) || 20;
  state.ui.sub2apiAccountPager.page = 1;
  loadSub2ApiAccounts();
}

function refreshSub2ApiSelectionState() {
  const accounts = state.ui.sub2apiAccounts || [];
  const visibleIds = accounts.map(a => a.id).filter(id => Number.isInteger(id) && id > 0);
  const selectedVisible = visibleIds.filter(id => state.ui.selectedSub2ApiAccountIds.has(id)).length;
  const selectedTotal = state.ui.selectedSub2ApiAccountIds.size;
  if (DOM.sub2apiAccountSelection)
    DOM.sub2apiAccountSelection.textContent = `已选 ${selectedTotal} 个`;
  if (DOM.sub2apiAccountSelectAll) {
    DOM.sub2apiAccountSelectAll.checked = visibleIds.length > 0 && selectedVisible === visibleIds.length;
    DOM.sub2apiAccountSelectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleIds.length;
  }
}

function toggleSelectAllSub2ApiAccounts() {
  const shouldSelect = !!DOM.sub2apiAccountSelectAll?.checked;
  (state.ui.sub2apiAccounts || []).forEach(a => {
    const id = Number(a.id || 0);
    if (id <= 0) return;
    if (shouldSelect) state.ui.selectedSub2ApiAccountIds.add(id);
    else state.ui.selectedSub2ApiAccountIds.delete(id);
  });
  renderSub2ApiAccountList();
}

function getSelectedIds() {
  return Array.from(state.ui.selectedSub2ApiAccountIds).filter(id => Number.isInteger(id) && id > 0);
}

function setSub2ApiAccountBusy(busy) {
  state.ui.sub2apiAccountActionBusy = busy;
  [
    DOM.sub2apiAccountApplyBtn, DOM.sub2apiAccountResetBtn, DOM.sub2apiAccountProbeBtn,
    DOM.sub2apiAccountExceptionBtn, DOM.sub2apiDuplicateScanBtn, DOM.sub2apiDuplicateCleanBtn,
    DOM.sub2apiAccountDeleteBtn, DOM.sub2apiAccountPrevBtn, DOM.sub2apiAccountNextBtn,
  ].forEach(btn => { if (btn) btn.disabled = busy; });
  if (!busy) updateSub2ApiPagerUI();
}

async function runSub2ApiAccountProbe(ids, label = '选中账号') {
  if (state.ui.sub2apiAccountActionBusy) return;
  if (!ids.length) { showToast('请先选择账号', 'error'); return; }
  setSub2ApiAccountBusy(true);
  if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = `正在测活 ${ids.length} 个账号...`;
  try {
    const res = await fetch('/api/sub2api/accounts/probe', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ account_ids: ids }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || '测活失败');
    const msg = `${label}: 刷新成功 ${data.refreshed_ok || 0}, 恢复 ${data.recovered || 0}, 仍异常 ${data.still_abnormal || 0}`;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'success');
    await loadSub2ApiAccounts({ silent: true });
    pollSub2ApiPoolStatus();
  } catch (e) {
    const msg = '测活失败: ' + e.message;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'error');
  } finally { setSub2ApiAccountBusy(false); }
}

async function triggerSelectedSub2ApiProbe() {
  await runSub2ApiAccountProbe(getSelectedIds());
}

async function triggerSub2ApiExceptionHandling() {
  if (state.ui.sub2apiAccountActionBusy) return;
  const ids = getSelectedIds();
  const confirmMsg = ids.length
    ? `确认处理 ${ids.length} 个已选账号？先测活，仍异常则删除。`
    : '未选择账号，将处理整个池中的异常账号。是否继续？';
  if (!confirm(confirmMsg)) return;
  setSub2ApiAccountBusy(true);
  if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = '正在处理异常账号...';
  try {
    const res = await fetch('/api/sub2api/accounts/handle-exception', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ account_ids: ids, delete_unresolved: true }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || '处理失败');
    const msg = `处理完成: 目标 ${data.targeted || 0}, 恢复 ${data.recovered || 0}, 删除 ${data.deleted_ok || 0}, 失败 ${data.deleted_fail || 0}`;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'success');
    await loadSub2ApiAccounts({ silent: true });
    pollSub2ApiPoolStatus();
  } catch (e) {
    const msg = '处理失败: ' + e.message;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'error');
  } finally { setSub2ApiAccountBusy(false); }
}

async function runSub2ApiAccountDelete(ids, label = '选中账号', requireConfirm = true) {
  if (state.ui.sub2apiAccountActionBusy) return;
  if (!ids.length) { showToast('请先选择账号', 'error'); return; }
  if (requireConfirm && !confirm(`确认删除 ${label}（共 ${ids.length} 个）？`)) return;
  setSub2ApiAccountBusy(true);
  if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = `正在删除 ${ids.length} 个账号...`;
  try {
    const res = await fetch('/api/sub2api/accounts/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ account_ids: ids }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || '删除失败');
    ids.forEach(id => state.ui.selectedSub2ApiAccountIds.delete(id));
    const msg = `删除完成: 成功 ${data.deleted_ok || 0}, 失败 ${data.deleted_fail || 0}`;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'success');
    await loadSub2ApiAccounts({ silent: true });
    pollSub2ApiPoolStatus();
  } catch (e) {
    const msg = '删除失败: ' + e.message;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'error');
  } finally { setSub2ApiAccountBusy(false); }
}

async function triggerSelectedSub2ApiDelete() {
  await runSub2ApiAccountDelete(getSelectedIds());
}

async function previewSub2ApiDuplicates() {
  if (state.ui.sub2apiAccountActionBusy) return;
  setSub2ApiAccountBusy(true);
  if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = '正在检测重复账号...';
  try {
    const res = await fetch('/api/sub2api/pool/dedupe', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: true }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || '检测失败');
    const msg = `重复预检: 重复组 ${data.duplicate_groups || 0}, 重复账号 ${data.duplicate_accounts || 0}, 可删 ${data.to_delete || 0}`;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'success');
    await loadSub2ApiAccounts({ silent: true });
  } catch (e) {
    const msg = '检测失败: ' + e.message;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'error');
  } finally { setSub2ApiAccountBusy(false); }
}

async function cleanupSub2ApiDuplicates() {
  if (state.ui.sub2apiAccountActionBusy) return;
  if (!confirm('确认清理重复账号？将保留每组最新账号，其余删除。')) return;
  setSub2ApiAccountBusy(true);
  if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = '正在清理重复账号...';
  try {
    const res = await fetch('/api/sub2api/pool/dedupe', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: false }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || '清理失败');
    const msg = `重复清理完成: 删除成功 ${data.deleted_ok || 0}, 失败 ${data.deleted_fail || 0}`;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'success');
    await loadSub2ApiAccounts({ silent: true });
    pollSub2ApiPoolStatus();
  } catch (e) {
    const msg = '清理失败: ' + e.message;
    if (DOM.sub2apiAccountActionStatus) DOM.sub2apiAccountActionStatus.textContent = msg;
    showToast(msg, 'error');
  } finally { setSub2ApiAccountBusy(false); }
}

// ==========================================
// Config
// ==========================================
async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const cfg = await res.json();
    if (DOM.duckmailApiBase) DOM.duckmailApiBase.value = cfg.duckmail_api_base || '';
    if (DOM.duckmailBearer && cfg.duckmail_bearer_preview) DOM.duckmailBearer.placeholder = '已配置，填写则更新，否则保留原值';
    if (DOM.proxyListUrl) DOM.proxyListUrl.value = cfg.proxy_list_url || '';
    if (DOM.proxyListBearer && cfg.proxy_list_bearer_preview) DOM.proxyListBearer.placeholder = '已配置，填写则更新，否则保留原值';
    if (DOM.proxyInput) DOM.proxyInput.value = cfg.proxy || '';
    if (DOM.stableProxyInput) DOM.stableProxyInput.value = cfg.stable_proxy || '';
    if (DOM.proxyValidateTimeout) DOM.proxyValidateTimeout.value = cfg.proxy_validate_timeout_seconds || 6;
    if (DOM.proxyValidateWorkers) DOM.proxyValidateWorkers.value = cfg.proxy_validate_workers || 40;
    if (DOM.proxyValidateEnabled) DOM.proxyValidateEnabled.checked = cfg.proxy_validate_enabled !== false;
    if (DOM.preferStableProxy) DOM.preferStableProxy.checked = cfg.prefer_stable_proxy !== false;
    if (DOM.sub2apiBaseUrl) DOM.sub2apiBaseUrl.value = cfg.sub2api_base_url || '';
    if (DOM.sub2apiMinCandidates) DOM.sub2apiMinCandidates.value = cfg.sub2api_min_candidates || 200;
    if (DOM.sub2apiEmail) DOM.sub2apiEmail.value = cfg.sub2api_email || '';
    if (DOM.sub2apiGroupIds) {
      const ids = Array.isArray(cfg.sub2api_group_ids) ? cfg.sub2api_group_ids.join(',') : String(cfg.sub2api_group_ids || '');
      DOM.sub2apiGroupIds.value = ids;
    }
    if (DOM.sub2apiProxyId) DOM.sub2apiProxyId.value = cfg.sub2api_proxy_id || '';
    if (DOM.sub2apiProxyName) DOM.sub2apiProxyName.value = cfg.sub2api_proxy_name || '';
    if (DOM.autoUploadSub2api) DOM.autoUploadSub2api.checked = !!cfg.auto_upload_sub2api;
    if (DOM.sub2apiAutoAssignProxy) DOM.sub2apiAutoAssignProxy.checked = !!cfg.sub2api_auto_assign_proxy;
    if (DOM.cpaBaseUrl) DOM.cpaBaseUrl.value = cfg.cpa_base_url || '';
    if (DOM.cpaMinCandidates) DOM.cpaMinCandidates.value = cfg.cpa_min_candidates || 200;
    if (DOM.autoUploadCpa) DOM.autoUploadCpa.checked = !!cfg.auto_upload_cpa;
    if (DOM.totalAccountsInput) DOM.totalAccountsInput.value = cfg.total_accounts || 3;
  } catch {}
}

async function saveDuckmailConfig() {
  DOM.duckmailSaveBtn.disabled = true;
  try {
    const body = { duckmail_api_base: DOM.duckmailApiBase?.value.trim() || '' };
    const bearer = DOM.duckmailBearer?.value.trim();
    if (bearer) body.duckmail_bearer = bearer;
    const res = await fetch('/api/config', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.ok) {
      showToast('临时邮箱配置已保存', 'success');
      if (DOM.duckmailStatus) DOM.duckmailStatus.textContent = '已保存';
      if (DOM.duckmailBearer) DOM.duckmailBearer.value = '';
    } else {
      showToast('保存失败', 'error');
    }
  } catch (e) { showToast('保存失败: ' + e.message, 'error'); }
  finally { DOM.duckmailSaveBtn.disabled = false; }
}

async function saveProxyConfig() {
  DOM.proxySaveBtn.disabled = true;
  try {
    const body = {
      proxy_list_url: DOM.proxyListUrl?.value.trim() || '',
      proxy: DOM.proxyInput?.value.trim() || '',
      stable_proxy: DOM.stableProxyInput?.value.trim() || '',
      proxy_validate_timeout_seconds: parseFloat(DOM.proxyValidateTimeout?.value || '6') || 6,
      proxy_validate_workers: parseInt(DOM.proxyValidateWorkers?.value || '40', 10) || 40,
      proxy_validate_enabled: DOM.proxyValidateEnabled?.checked !== false,
      prefer_stable_proxy: DOM.preferStableProxy?.checked !== false,
    };
    const proxyListBearer = DOM.proxyListBearer?.value.trim();
    if (proxyListBearer) body.proxy_list_bearer = proxyListBearer;
    const res = await fetch('/api/config', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.ok) {
      showToast('代理配置已保存', 'success');
      if (DOM.proxyStatus) DOM.proxyStatus.textContent = '已保存';
      if (DOM.proxyListBearer) DOM.proxyListBearer.value = '';
    } else {
      showToast('保存失败', 'error');
    }
  } catch (e) { showToast('保存失败: ' + e.message, 'error'); }
  finally { DOM.proxySaveBtn.disabled = false; }
}

async function saveSub2ApiConfig() {
  DOM.sub2apiSaveBtn.disabled = true;
  try {
    const groupIdsRaw = DOM.sub2apiGroupIds?.value.trim() || '2';
    const groupIds = groupIdsRaw.split(',').map(s => parseInt(s.trim(), 10)).filter(n => Number.isFinite(n) && n > 0);
    const body = {
      sub2api_base_url: DOM.sub2apiBaseUrl?.value.trim() || '',
      sub2api_email: DOM.sub2apiEmail?.value.trim() || '',
      sub2api_min_candidates: parseInt(DOM.sub2apiMinCandidates?.value || '200', 10) || 200,
      sub2api_group_ids: groupIds.length ? groupIds : [2],
      sub2api_proxy_id: parseInt(DOM.sub2apiProxyId?.value || '0', 10) || 0,
      sub2api_proxy_name: DOM.sub2apiProxyName?.value.trim() || '',
      auto_upload_sub2api: DOM.autoUploadSub2api?.checked || false,
      sub2api_auto_assign_proxy: DOM.sub2apiAutoAssignProxy?.checked || false,
    };
    const pwd = DOM.sub2apiPassword?.value.trim();
    if (pwd && pwd !== '**masked**') body.sub2api_password = pwd;
    const res = await fetch('/api/config', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.ok) {
      showToast('Sub2Api 配置已保存', 'success');
      if (DOM.sub2apiConfigStatus) DOM.sub2apiConfigStatus.textContent = '已保存';
      if (DOM.sub2apiPassword) DOM.sub2apiPassword.value = '';
      pollSub2ApiPoolStatus();
      loadSub2ApiAccounts();
    } else {
      const data = await res.json();
      showToast(data.detail || '保存失败', 'error');
    }
  } catch (e) { showToast('保存失败: ' + e.message, 'error'); }
  finally { DOM.sub2apiSaveBtn.disabled = false; }
}

// ==========================================
// CPA Pool & Accounts
// ==========================================

async function pollCpaPoolStatus() {
  try {
    const res = await fetch('/api/cpa/pool/status');
    const data = await res.json();
    if (!data.configured) {
      ['cpaPoolTotal', 'cpaPoolNormal', 'cpaPoolError', 'cpaPoolThreshold', 'cpaPoolPercent'].forEach(id => {
        const el = $(id); if (el) el.textContent = '--';
      });
      updateHeaderCpa(null);
      return;
    }
    const normal = data.candidates || 0;
    const error = data.error_count || 0;
    const total = data.total || 0;
    const threshold = data.threshold || 0;
    const fillPct = threshold > 0 ? Math.round(normal / threshold * 100) : 100;

    if (DOM.cpaPoolTotal) DOM.cpaPoolTotal.textContent = total;
    if (DOM.cpaPoolNormal) DOM.cpaPoolNormal.textContent = normal;
    if (DOM.cpaPoolError) {
      DOM.cpaPoolError.textContent = error;
      DOM.cpaPoolError.className = `stat-value ${error > 0 ? 'red' : 'green'}`;
    }
    if (DOM.cpaPoolThreshold) DOM.cpaPoolThreshold.textContent = threshold;
    if (DOM.cpaPoolPercent) {
      DOM.cpaPoolPercent.textContent = fillPct + '%';
      DOM.cpaPoolPercent.className = `stat-value ${fillPct >= 100 ? 'green' : fillPct >= 80 ? '' : 'red'}`;
    }
    updateHeaderCpa({ normal, threshold, fillPct, error });
  } catch {}
}

function updateHeaderCpa(data) {
  if (!data) {
    if (DOM.headerCpaLabel) DOM.headerCpaLabel.textContent = '-- / --';
    if (DOM.headerCpaDelta) DOM.headerCpaDelta.textContent = '--';
    if (DOM.headerCpaBar) DOM.headerCpaBar.style.width = '0%';
    setChipStatus(DOM.headerCpaChip, 'idle');
    return;
  }
  const { normal, threshold, fillPct, error } = data;
  const st = error > 0 ? 'danger' : (fillPct > 110 ? 'over' : fillPct >= 100 ? 'ok' : fillPct >= 80 ? 'warn' : 'danger');
  if (DOM.headerCpaLabel) DOM.headerCpaLabel.textContent = `${normal} / ${threshold}`;
  if (DOM.headerCpaDelta) {
    const delta = Math.round(fillPct - 100);
    DOM.headerCpaDelta.textContent = delta === 0 ? '0%' : `${delta > 0 ? '+' : ''}${delta}%`;
    DOM.headerCpaDelta.className = `pool-chip-delta ${st}`;
  }
  if (DOM.headerCpaBar) {
    DOM.headerCpaBar.style.width = Math.min(100, fillPct) + '%';
    DOM.headerCpaBar.className = `pool-chip-fill ${st}`;
  }
  setChipStatus(DOM.headerCpaChip, st);
}

const CPA_ABNORMAL = new Set(['error', 'disabled', 'unavailable']);

function applyCpaAccountFilter() {
  state.ui.cpaAccountFilter.status = DOM.cpaAccountStatusFilter?.value || 'all';
  state.ui.cpaAccountFilter.keyword = DOM.cpaAccountKeyword?.value.trim() || '';
  state.ui.cpaAccountPager.page = 1;
  loadCpaAccounts();
}

function resetCpaAccountFilter() {
  state.ui.cpaAccountFilter = { status: 'all', keyword: '' };
  if (DOM.cpaAccountStatusFilter) DOM.cpaAccountStatusFilter.value = 'all';
  if (DOM.cpaAccountKeyword) DOM.cpaAccountKeyword.value = '';
  state.ui.cpaAccountPager.page = 1;
  loadCpaAccounts();
}

async function loadCpaAccounts({ silent = false } = {}) {
  if (!DOM.cpaAccountList || state.ui.cpaAccountsLoading) return;
  state.ui.cpaAccountsLoading = true;
  try {
    const p = state.ui.cpaAccountPager;
    const f = state.ui.cpaAccountFilter;
    const params = new URLSearchParams({
      page: String(p.page || 1),
      page_size: String(p.pageSize || 20),
      status: String(f.status || 'all'),
      keyword: String(f.keyword || ''),
    });
    const res = await fetch(`/api/cpa/accounts?${params}`);
    const data = await res.json();
    if (!data.configured) {
      state.ui.cpaAccounts = [];
      renderCpaAccountList('CPA 未配置');
      return;
    }
    state.ui.cpaAccounts = Array.isArray(data.items) ? data.items : [];
    state.ui.cpaAccountPager = {
      page: parseInt(data.page, 10) || 1,
      pageSize: parseInt(data.page_size, 10) || 20,
      total: parseInt(data.total, 10) || 0,
      filteredTotal: parseInt(data.filtered_total, 10) || 0,
      totalPages: parseInt(data.total_pages, 10) || 1,
    };
    renderCpaAccountList();
  } catch (e) {
    if (!silent) renderCpaAccountList('加载失败');
  } finally {
    state.ui.cpaAccountsLoading = false;
    refreshCpaSelectionState();
  }
}

function renderCpaAccountList(emptyMessage = '') {
  const accounts = state.ui.cpaAccounts || [];
  if (!accounts.length) {
    DOM.cpaAccountList.innerHTML = `<div class="empty-state"><div class="empty-icon">\u25A1</div><span>${escapeHtml(emptyMessage || '暂无 auth file')}</span></div>`;
  } else {
    DOM.cpaAccountList.innerHTML = accounts.map(renderCpaAccountItem).join('');
  }
  updateCpaPagerUI();
  refreshCpaSelectionState();
}

function renderCpaAccountItem(item) {
  const name = item.name || '';
  const email = item.email || name.replace('.json', '');
  const status = String(item.status || 'unknown').toLowerCase();
  const disabled = item.disabled;
  const unavailable = item.unavailable;
  const isAbnormal = CPA_ABNORMAL.has(status) || disabled || unavailable;
  const selected = state.ui.selectedCpaAccountNames.has(name);

  let statusLabel = status;
  let statusClass = 'ok';
  if (disabled) { statusLabel = 'disabled'; statusClass = 'warn'; }
  else if (unavailable) { statusLabel = 'unavailable'; statusClass = 'danger'; }
  else if (status === 'error') { statusLabel = '异常'; statusClass = 'danger'; }
  else if (!isAbnormal) { statusLabel = '正常'; statusClass = 'ok'; }

  const msg = item.status_message ? ` title="${escapeHtml(item.status_message)}"` : '';

  return `<div class="token-item cpa-account-item${selected ? ' selected' : ''}" id="cpa-account-${escapeHtml(name)}">
  <label class="account-check-wrap"><input type="checkbox" class="cpa-account-check" data-file-name="${escapeHtml(name)}" ${selected ? 'checked' : ''}></label>
  <div class="token-info">
    <div class="token-email">
      <span class="token-email-text" title="${escapeHtml(email)}">${escapeHtml(email)}</span>
      <span class="account-status-badge ${statusClass}"${msg}>${escapeHtml(statusLabel)}</span>
    </div>
    <div class="token-meta">${escapeHtml(name)} \u00b7 ${escapeHtml(formatTime(item.updated_at))}</div>
  </div>
  <div class="token-actions">
    <button class="btn btn-danger btn-sm cpa-account-delete-btn" data-file-name="${escapeHtml(name)}" data-email="${encodeURIComponent(email)}">删除</button>
  </div>
</div>`;
}

function updateCpaPagerUI() {
  const p = state.ui.cpaAccountPager;
  if (DOM.cpaAccountPageInfo) DOM.cpaAccountPageInfo.textContent = `第 ${p.page}/${p.totalPages} 页 \u00b7 共 ${p.filteredTotal} 条`;
  if (DOM.cpaAccountPrevBtn) DOM.cpaAccountPrevBtn.disabled = p.page <= 1;
  if (DOM.cpaAccountNextBtn) DOM.cpaAccountNextBtn.disabled = p.page >= p.totalPages;
}

function changeCpaAccountPage(delta) {
  const p = state.ui.cpaAccountPager;
  const next = p.page + delta;
  if (next < 1 || next > p.totalPages) return;
  p.page = next;
  loadCpaAccounts();
}

function changeCpaAccountPageSize() {
  state.ui.cpaAccountPager.pageSize = parseInt(DOM.cpaAccountPageSize?.value || '20', 10) || 20;
  state.ui.cpaAccountPager.page = 1;
  loadCpaAccounts();
}

function refreshCpaSelectionState() {
  const count = state.ui.selectedCpaAccountNames.size;
  if (DOM.cpaAccountSelection) DOM.cpaAccountSelection.textContent = count > 0 ? `已选 ${count} 个` : '';
  if (DOM.cpaAccountSelectAll) {
    const items = state.ui.cpaAccounts || [];
    DOM.cpaAccountSelectAll.checked = items.length > 0 && items.every(a => state.ui.selectedCpaAccountNames.has(a.name));
  }
}

function toggleSelectAllCpaAccounts() {
  const checked = DOM.cpaAccountSelectAll?.checked;
  const items = state.ui.cpaAccounts || [];
  if (checked) items.forEach(a => { if (a.name) state.ui.selectedCpaAccountNames.add(a.name); });
  else items.forEach(a => state.ui.selectedCpaAccountNames.delete(a.name));
  renderCpaAccountList();
}

function setCpaAccountBusy(busy) {
  state.ui.cpaAccountActionBusy = busy;
  if (DOM.cpaAccountDeleteBtn) DOM.cpaAccountDeleteBtn.disabled = busy;
}

async function runCpaAccountDelete(names, label = '选中 auth file') {
  if (!names.length) return;
  if (!confirm(`确定删除 ${names.length > 1 ? names.length + ' 个 auth file' : label}？`)) return;
  setCpaAccountBusy(true);
  if (DOM.cpaAccountActionStatus) DOM.cpaAccountActionStatus.textContent = '删除中...';
  try {
    const res = await fetch('/api/cpa/accounts/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filenames: names }),
    });
    const data = await res.json();
    const msg = `删除成功 ${data.deleted_ok || 0} 个，失败 ${data.deleted_fail || 0} 个`;
    showToast(msg, data.deleted_fail > 0 ? 'warn' : 'success');
    if (DOM.cpaAccountActionStatus) DOM.cpaAccountActionStatus.textContent = msg;
    names.forEach(n => state.ui.selectedCpaAccountNames.delete(n));
    await loadCpaAccounts();
    pollCpaPoolStatus();
  } catch (e) {
    showToast('删除失败: ' + e.message, 'error');
    if (DOM.cpaAccountActionStatus) DOM.cpaAccountActionStatus.textContent = '删除失败';
  } finally {
    setCpaAccountBusy(false);
  }
}

function triggerSelectedCpaDelete() {
  const names = Array.from(state.ui.selectedCpaAccountNames);
  if (!names.length) { showToast('请先选择要删除的 auth file', 'warn'); return; }
  runCpaAccountDelete(names, `${names.length} 个 auth file`);
}

async function testCpaConnection() {
  if (DOM.cpaTestBtn) DOM.cpaTestBtn.disabled = true;
  if (DOM.cpaConfigStatus) DOM.cpaConfigStatus.textContent = '测试中...';
  try {
    const res = await fetch('/api/cpa/pool/check', { method: 'POST' });
    const data = await res.json();
    if (DOM.cpaConfigStatus) DOM.cpaConfigStatus.textContent = data.message || (data.ok ? '连接成功' : '连接失败');
    showToast(data.ok ? 'CPA 连接成功' : 'CPA 连接失败', data.ok ? 'success' : 'error');
  } catch (e) {
    if (DOM.cpaConfigStatus) DOM.cpaConfigStatus.textContent = '请求失败: ' + e.message;
  }
  if (DOM.cpaTestBtn) DOM.cpaTestBtn.disabled = false;
}

async function saveCpaConfig() {
  if (DOM.cpaSaveBtn) DOM.cpaSaveBtn.disabled = true;
  try {
    const body = {
      cpa_base_url: DOM.cpaBaseUrl?.value.trim() || '',
      cpa_min_candidates: parseInt(DOM.cpaMinCandidates?.value || '200', 10) || 200,
      auto_upload_cpa: DOM.autoUploadCpa?.checked || false,
    };
    const key = DOM.cpaManagementKey?.value.trim();
    if (key && key !== '**masked**') body.cpa_management_key = key;
    const res = await fetch('/api/config', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.ok) {
      showToast('CPA 配置已保存', 'success');
      if (DOM.cpaConfigStatus) DOM.cpaConfigStatus.textContent = '已保存';
      if (DOM.cpaManagementKey) DOM.cpaManagementKey.value = '';
      pollCpaPoolStatus();
      loadCpaAccounts();
    } else {
      showToast('保存失败', 'error');
    }
  } catch (e) { showToast('保存失败: ' + e.message, 'error'); }
  finally { if (DOM.cpaSaveBtn) DOM.cpaSaveBtn.disabled = false; }
}

// ==========================================
// Theme
// ==========================================
const THEME_KEY = 'chatgpt_register_theme_v1';

function initThemeSwitch() {
  const btn = DOM.themeToggleBtn;
  if (!btn) return;
  let saved = 'dark';
  try { const v = localStorage.getItem(THEME_KEY); if (v === 'light' || v === 'dark') saved = v; } catch {}
  applyTheme(saved);
  btn.addEventListener('click', () => {
    const next = document.body.classList.contains('theme-light') ? 'dark' : 'light';
    applyTheme(next);
    try { localStorage.setItem(THEME_KEY, next); } catch {}
  });
}

function applyTheme(theme) {
  const isLight = theme === 'light';
  document.body.classList.toggle('theme-light', isLight);
  const btn = DOM.themeToggleBtn;
  if (!btn) return;
  const lbl = btn.querySelector('.theme-toggle-label');
  if (lbl) lbl.textContent = isLight ? '明亮' : '黑暗';
  btn.setAttribute('aria-label', `切换到${isLight ? '黑暗' : '明亮'}主题`);
}

// ==========================================
// Toast
// ==========================================
const TOAST_ICONS = { success: '&#10003;', error: '&#10007;', info: '&#8505;' };

function showToast(msg, type = 'info') {
  const container = $('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span class="toast-icon">${TOAST_ICONS[type] || TOAST_ICONS.info}</span><span>${escapeHtml(msg)}</span>`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.animation = 'toast-out .25s var(--ease-spring) forwards';
    toast.addEventListener('animationend', () => toast.remove());
  }, 3200);
}

// ==========================================
// Utils
// ==========================================
function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function formatTime(timeStr) {
  if (!timeStr) return '--';
  try {
    const d = new Date(timeStr);
    if (isNaN(d)) return timeStr;
    const pad = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  } catch { return timeStr; }
}

// ==========================================
// Drag resize + localStorage
// ==========================================
(function initResizable() {
  const STORAGE_KEY = 'chatgpt_register_layout_v1';
  const shell = document.querySelector('.app-shell');
  const resizeLeft = document.getElementById('resizeLeft');
  const resizeRight = document.getElementById('resizeRight');
  if (!shell) return;

  function getTrackPx(index) {
    const tracks = getComputedStyle(shell).gridTemplateColumns.match(/[\d.]+px/g) || [];
    const val = tracks[index] ? parseFloat(tracks[index]) : NaN;
    return Number.isFinite(val) ? val : NaN;
  }

  function loadLayout() {
    try {
      const saved = JSON.parse(localStorage.getItem(STORAGE_KEY));
      if (!saved) return;
      const maxW = shell.getBoundingClientRect().width || window.innerWidth;
      if (saved.left >= 200 && saved.left <= maxW * 0.4) shell.style.setProperty('--col-left', saved.left + 'px');
      if (saved.right >= 240 && saved.right <= maxW * 0.4) shell.style.setProperty('--col-right', saved.right + 'px');
    } catch {}
  }

  function saveLayout() {
    const data = {};
    const left = getTrackPx(0); if (Number.isFinite(left) && left > 0) data.left = left;
    const right = getTrackPx(4); if (Number.isFinite(right) && right > 0) data.right = right;
    if (Object.keys(data).length) try { localStorage.setItem(STORAGE_KEY, JSON.stringify(data)); } catch {}
  }

  function initHandle(handle, prop, minW, getStart) {
    if (!handle) return;
    handle.addEventListener('mousedown', e => {
      e.preventDefault();
      document.body.classList.add('resizing');
      handle.classList.add('active');
      const startX = e.clientX;
      const startVal = getStart();
      const totalW = shell.getBoundingClientRect().width;
      const onMove = ev => {
        const delta = prop === '--col-left' ? ev.clientX - startX : startX - ev.clientX;
        shell.style.setProperty(prop, Math.max(minW, Math.min(startVal + delta, totalW * 0.4)) + 'px');
      };
      const onUp = () => {
        document.body.classList.remove('resizing');
        handle.classList.remove('active');
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        saveLayout();
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  initHandle(resizeLeft, '--col-left', 200, () => getTrackPx(0) || 260);
  initHandle(resizeRight, '--col-right', 240, () => getTrackPx(4) || 340);
  loadLayout();
})();
