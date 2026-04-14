const state = {
  latestAnswers: [],
  currentQuestion: "",
  history: [],
  officialQuestions: [],
  systemQuestions: [],
  customQuestions: [],
  questionFilterTags: [],
  activeQuestionTag: "\u5168\u90e8",
  questionSourceFilter: "all",
  questionTypeFilter: "all",
  selectedQuestionKeys: [],
  files: null,
  tables: [],
  selectedTable: "",
  tablePage: 1,
  tableTotal: 0,
  editingQuestionId: "",
  evidenceEntries: [],
  historyExpanded: false,
  busy: {
    ask: false,
    upload: false,
    rebuild: false,
    export: false,
  },
  databaseReady: false,
  databaseRebuilding: false,
};

const refs = {
  navItems: [...document.querySelectorAll(".nav-item")],
  views: [...document.querySelectorAll(".view-panel")],
  overviewStats: document.getElementById("overviewStats"),
  sourceSummary: document.getElementById("sourceSummary"),
  questionInput: document.getElementById("questionInput"),
  askButton: document.getElementById("askButton"),
  loadSamplesButton: document.getElementById("loadSamplesButton"),
  resetContextButton: document.getElementById("resetContextButton"),
  exportLatestButton: document.getElementById("exportLatestButton"),
  askStatus: document.getElementById("askStatus"),
  samplesBox: document.getElementById("samplesBox"),
  resultsBox: document.getElementById("resultsBox"),
  historyList: document.getElementById("historyList"),
  clearHistoryButton: document.getElementById("clearHistoryButton"),
  exportNotice: document.getElementById("exportNotice"),
  uploadCategory: document.getElementById("uploadCategory"),
  uploadFile: document.getElementById("uploadFile"),
  uploadButton: document.getElementById("uploadButton"),
  rebuildButton: document.getElementById("rebuildButton"),
  rebuildTopButton: document.getElementById("rebuildTopButton"),
  exportHistoryButton: document.getElementById("exportHistoryButton"),
  uploadStatus: document.getElementById("uploadStatus"),
  fileSections: document.getElementById("fileSections"),
  questionTitle: document.getElementById("questionTitle"),
  questionPayload: document.getElementById("questionPayload"),
  questionTags: document.getElementById("questionTags"),
  questionNote: document.getElementById("questionNote"),
  saveQuestionButton: document.getElementById("saveQuestionButton"),
  resetQuestionButton: document.getElementById("resetQuestionButton"),
  questionStatus: document.getElementById("questionStatus"),
  questionPanelTitle: document.getElementById("questionPanelTitle"),
  questionOverview: document.getElementById("questionOverview"),
  questionSearch: document.getElementById("questionSearch"),
  questionSourceFilter: document.getElementById("questionSourceFilter"),
  questionTypeFilter: document.getElementById("questionTypeFilter"),
  questionTagBar: document.getElementById("questionTagBar"),
  questionTagSummary: document.getElementById("questionTagSummary"),
  questionLibrary: document.getElementById("questionLibrary"),
  newQuestionButton: document.getElementById("newQuestionButton"),
  clearQuestionFiltersButton: document.getElementById("clearQuestionFiltersButton"),
  deleteSelectedQuestionsButton: document.getElementById("deleteSelectedQuestionsButton"),
  tableList: document.getElementById("tableList"),
  tableTitle: document.getElementById("tableTitle"),
  tableMeta: document.getElementById("tableMeta"),
  tablePreview: document.getElementById("tablePreview"),
  prevTablePageButton: document.getElementById("prevTablePageButton"),
  nextTablePageButton: document.getElementById("nextTablePageButton"),
  evidenceList: document.getElementById("evidenceList"),
  previewTitle: document.getElementById("previewTitle"),
  previewMeta: document.getElementById("previewMeta"),
  previewBody: document.getElementById("previewBody"),
};

const QUESTION_TYPE_ORDER = [
  "\u6570\u636e\u57fa\u672c\u67e5\u8be2",
  "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2",
  "\u591a\u610f\u56fe",
  "\u610f\u56fe\u6a21\u7cca",
  "\u5f52\u56e0\u5206\u6790",
  "\u5f00\u653e\u6027\u95ee\u9898",
  "\u878d\u5408\u67e5\u8be2",
  "\u6570\u636e\u6821\u9a8c",
];

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function basename(value) {
  const normalized = String(value || "").replaceAll("\\", "/");
  return normalized.split("/").pop() || normalized;
}

function isPdfReference(value) {
  return String(value || "").trim().toLowerCase().includes(".pdf");
}

async function api(url, options = {}) {
  try {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({ error: "服务返回了不可解析的响应" }));
    if (!response.ok && !data.error) {
      data.error = `请求失败：${response.status}`;
    }
    if (!data.message && data.error) {
      data.message = data.error;
    }
    return data;
  } catch (_error) {
    return { error: "网络请求失败，请稍后重试。", message: "网络请求失败，请稍后重试。" };
  }
}

function setStatus(node, message, isError = false) {
  node.textContent = message || "";
  node.classList.toggle("error", Boolean(message) && isError);
}

function responseMessage(data, fallback = "请求失败，请稍后重试。") {
  return data?.message || data?.error || fallback;
}

function setButtonGroupDisabled(buttons, disabled) {
  (buttons || []).forEach((button) => {
    if (button) {
      button.disabled = disabled;
    }
  });
}

function syncActionAvailability() {
  const rebuilding = Boolean(state.databaseRebuilding || state.busy.rebuild);
  setButtonGroupDisabled([refs.askButton], !state.databaseReady || rebuilding || state.busy.ask);
  setButtonGroupDisabled([refs.uploadButton], rebuilding || state.busy.upload);
  setButtonGroupDisabled([refs.rebuildButton, refs.rebuildTopButton], rebuilding);
  setButtonGroupDisabled([refs.exportLatestButton, refs.exportHistoryButton], state.busy.export);
}

async function withBusy(action, options, executor) {
  if (state.busy[action]) {
    return null;
  }
  const { buttons = [], statusNode = null, loadingText = "" } = options || {};
  state.busy[action] = true;
  setButtonGroupDisabled(buttons, true);
  if (statusNode && loadingText) {
    setStatus(statusNode, loadingText);
  }
  syncActionAvailability();
  try {
    return await executor();
  } finally {
    state.busy[action] = false;
    setButtonGroupDisabled(buttons, false);
    syncActionAvailability();
  }
}

function setView(viewName) {
  refs.navItems.forEach((item) => item.classList.toggle("active", item.dataset.view === viewName));
  refs.views.forEach((view) => view.classList.toggle("active", view.id === `view-${viewName}`));
}

function buildExportRecordsFromAnswers() {
  const askedAt = new Date().toISOString().slice(0, 19);
  return (state.latestAnswers || []).map((answer) => ({
    asked_at: askedAt,
    question: answer.q || state.currentQuestion,
    content: answer.a?.content || "",
    sql: answer.a?.sql || "",
    references: answer.a?.references || [],
    images: answer.a?.image || [],
  }));
}

function buildExportRecordsFromHistory() {
  const rows = [];
  (state.history || []).forEach((session) => {
    (session.answers || []).forEach((answer) => {
      rows.push({
        asked_at: session.asked_at || "",
        question: answer.q || session.raw_question || "",
        content: answer.a?.content || "",
        sql: answer.a?.sql || "",
        references: answer.a?.references || [],
        images: answer.a?.image || [],
      });
    });
  });
  return rows;
}

function buildPdfReferenceGroups(references) {
  const pdfGroups = new Map();
  const otherReferences = [];
  (references || []).forEach((ref) => {
    const path = String(ref.paper_path || "").trim();
    const text = String(ref.text || "").trim();
    if (!path) {
      return;
    }
    if (!isPdfReference(path)) {
      otherReferences.push({
        path,
        text: text || "\u672a\u8fd4\u56de\u5f15\u7528\u5185\u5bb9\u3002",
      });
      return;
    }
    if (!pdfGroups.has(path)) {
      pdfGroups.set(path, {
        ref: path,
        label: basename(path),
        path,
        snippets: [],
        _signatures: new Set(),
      });
    }
    const group = pdfGroups.get(path);
    const signature = text || "__empty__";
    if (group._signatures.has(signature)) {
      return;
    }
    group._signatures.add(signature);
    group.snippets.push(text || "\u8be5 PDF \u88ab\u7528\u4e8e\u56de\u7b54\uff0c\u4f46\u5f53\u524d\u672a\u8fd4\u56de\u66f4\u7ec6\u7684\u8bc1\u636e\u7247\u6bb5\u3002");
  });
  return {
    pdfGroups: [...pdfGroups.values()]
      .map(({ _signatures, ...item }) => item)
      .sort((left, right) => (right.snippets.length - left.snippets.length) || left.label.localeCompare(right.label, "zh-CN")),
    otherReferences,
  };
}

function buildReferenceSummary(references) {
  const { pdfGroups, otherReferences } = buildPdfReferenceGroups(references);
  const pdfSnippetCount = pdfGroups.reduce((total, group) => total + group.snippets.length, 0);
  const summary = [];
  if (pdfGroups.length) {
    summary.push(`\u547d\u4e2d ${pdfGroups.length} \u4e2a PDF \u6587\u4ef6`);
  }
  if (pdfSnippetCount) {
    summary.push(`${pdfSnippetCount} \u6761 PDF \u7247\u6bb5`);
  }
  if (otherReferences.length) {
    summary.push(`${otherReferences.length} \u6761\u975e PDF \u5f15\u7528`);
  }
  return summary.join(" \u00b7 ");
}

function renderHistoryCard(item) {
  return `
    <article class="history-card">
      <div>
        <strong>${escapeHtml(item.raw_question || "\u672a\u547d\u540d\u63d0\u95ee") }</strong>
        <p>${escapeHtml(item.summary || "")}</p>
        <small>${escapeHtml(item.asked_at || "")}</small>
      </div>
      <div class="inline-actions">
        <button class="mini-button" data-history-use="${escapeHtml(item.id)}">\u56de\u586b</button>
      </div>
    </article>
  `;
}

async function exportRecords(records, format = "xlsx") {
  if (!records?.length) {
    setStatus(refs.exportNotice, "当前没有可导出的结果", true);
    return;
  }
  await withBusy(
    "export",
    {
      buttons: [refs.exportLatestButton, refs.exportHistoryButton],
      statusNode: refs.exportNotice,
      loadingText: "正在导出…",
    },
    async () => {
      const data = await api("/api/export-results", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ format, records }),
      });
      if (data.error) {
        setStatus(refs.exportNotice, responseMessage(data), true);
        return;
      }
      setStatus(refs.exportNotice, data.message ? `${data.message} ${data.path}` : `已导出：${data.path}`);
      if (data.download_url) {
        window.open(data.download_url, "_blank", "noopener");
      }
      await Promise.all([loadOverview(), loadFiles()]);
    },
  );
}

function renderOverview(data) {
  const database = data.database || {};
  const databaseReady = Boolean(database.ready);
  state.databaseReady = databaseReady;
  state.databaseRebuilding = Boolean(database.rebuilding);
  const databaseStatus = database.status_text || (databaseReady ? "数据库已就绪" : "数据库未就绪");
  const databaseAction = database.action_text || "";
  refs.overviewStats.innerHTML = (data.stats || [])
    .map(
      (item) => `
        <article class="metric-card">
          <p>${escapeHtml(item.label)}</p>
          <strong>${escapeHtml(item.value)}</strong>
          <span>${escapeHtml(item.hint)}</span>
        </article>
      `,
    )
    .join("");

  refs.sourceSummary.innerHTML = `
    <div class="meta-line">数据库：${escapeHtml(database.path || "未构建")} · ${escapeHtml(database.size || "0B")} · ${escapeHtml(database.table_count || 0)} 张表 · ${escapeHtml(databaseStatus)}</div>
    ${(data.sources || [])
      .map(
        (item) => `
          <div class="info-item">
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(item.value)}</span>
            <small>${escapeHtml(item.path || "")}</small>
          </div>
        `,
      )
      .join("")}
    ${databaseReady ? "" : `
      <div class="tip-box">
        <strong>当前前端已可打开，但数据库尚未可用</strong>
        <p>${escapeHtml(`${databaseStatus} ${databaseAction}`.trim())}</p>
      </div>
    `}
  `;

  syncActionAvailability();
  if (!databaseReady && !state.databaseRebuilding) {
    setStatus(refs.askStatus, "当前数据库未按正式数据构建，请先点击“重建数据库”", true);
  } else if (state.databaseRebuilding && !state.busy.rebuild) {
    setStatus(refs.askStatus, "正在重建数据库，请稍候…", true);
    setStatus(refs.uploadStatus, "正在重建数据库，请稍候…");
  } else if (refs.askStatus.textContent.includes("未按正式数据构建") || refs.askStatus.textContent.includes("正在重建数据库")) {
    setStatus(refs.askStatus, "");
  }
}

async function loadOverview() {
  const data = await api("/api/overview");
  if (!data.error) {
    renderOverview(data);
  }
}

async function loadSamples() {
  const data = await api("/api/sample-questions");
  refs.samplesBox.innerHTML = (data.samples || [])
    .map((sample) => `<button class="chip-button" data-sample="${escapeHtml(sample)}">${escapeHtml(sample)}</button>`)
    .join("");
}

function renderResults() {
  if (!state.latestAnswers.length) {
    refs.resultsBox.innerHTML = '<div class=\"empty-box\">\u7ed3\u679c\u533a\u4e3a\u7a7a\u3002\u53ef\u4ee5\u5148\u4ece\u5de6\u4e0a\u89d2\u8f7d\u5165\u6837\u4f8b\u6216\u76f4\u63a5\u63d0\u95ee\u3002</div>';
    return;
  }
  refs.resultsBox.innerHTML = state.latestAnswers
    .map((item, index) => {
      const references = item.a?.references || [];
      const images = item.a?.image || [];
      const { pdfGroups, otherReferences } = buildPdfReferenceGroups(references);
      const referenceSummary = buildReferenceSummary(references);
      return `
        <article class="result-card">
          <div class="result-headline">
            <span class="result-index">${index + 1}</span>
            <div>
              <h4>${escapeHtml(item.q || "\u672a\u547d\u540d\u95ee\u9898") }</h4>
              <p>\u7ed3\u6784\u5316\u56de\u7b54\u3001SQL \u548c\u5f15\u7528\u8bc1\u636e\u7edf\u4e00\u5c55\u793a</p>
            </div>
          </div>
          <pre class="answer-block">${escapeHtml(item.a?.content || "")}</pre>
          ${item.a?.sql ? `<div class="sql-block"><span>SQL</span><pre>${escapeHtml(item.a.sql)}</pre></div>` : ""}
          ${images.length ? `<div class="image-grid">${images.map((image) => `<img src="/${encodeURI(image)}" alt="${escapeHtml(image)}" />`).join("")}</div>` : ""}
          ${references.length ? `<div class="reference-summary">${escapeHtml(referenceSummary || `\u5171 ${references.length} \u6761\u5f15\u7528`)}</div>` : ""}
          ${pdfGroups.length ? `<div class="reference-grid">${pdfGroups
              .map(
                (group) => `
                  <button class="reference-card pdf-group" data-ref="${escapeHtml(group.ref)}">
                    <strong>${escapeHtml(group.label)}</strong>
                    <span>${escapeHtml(group.snippets[0] || "\u8be5 PDF \u88ab\u7528\u4e8e\u56de\u7b54\uff0c\u4f46\u5f53\u524d\u672a\u8fd4\u56de\u66f4\u7ec6\u7684\u8bc1\u636e\u7247\u6bb5\u3002")}</span>
                    <small class="reference-note">${escapeHtml(group.path)} \u00b7 PDF \u6587\u4ef6 \u00b7 \u547d\u4e2d ${group.snippets.length} \u6761\u7247\u6bb5\uff0c\u70b9\u51fb\u8fdb\u5165\u8bc1\u636e\u9884\u89c8</small>
                  </button>
                `,
              )
              .join("")}</div>` : ""}
          ${otherReferences.length ? `<div class="reference-grid non-pdf-grid">${otherReferences
              .map(
                (ref) => `
                  <div class="reference-card static">
                    <strong>${escapeHtml(ref.path || "\u5176\u4ed6\u5f15\u7528") }</strong>
                    <span>${escapeHtml(ref.text || "\u672a\u8fd4\u56de\u5f15\u7528\u5185\u5bb9\u3002")}</span>
                    <small class="reference-note">\u5f53\u524d\u5f15\u7528\u4e0d\u662f PDF \u6587\u4ef6\uff0c\u4ec5\u5728\u7ed3\u679c\u533a\u5c55\u793a</small>
                  </div>
                `,
              )
              .join("")}</div>` : ""}
        </article>
      `;
    })
    .join("");
}

function renderHistory() {
  if (!state.history.length) {
    refs.historyList.innerHTML = '<div class=\"empty-box small\">\u6682\u65e0\u5386\u53f2\u8bb0\u5f55\u3002</div>';
    return;
  }

  const latestItems = state.history.slice(0, 3);
  const hiddenItems = state.history.slice(3);
  refs.historyList.innerHTML = `
    ${latestItems.map(renderHistoryCard).join("")}
    ${hiddenItems.length
      ? `
        <section class="history-collapse">
          <button class="text-button history-toggle" data-history-toggle="${state.historyExpanded ? "collapse" : "expand"}">
            ${state.historyExpanded ? "\u6536\u8d77\u66f4\u65e9\u8bb0\u5f55" : `\u5c55\u5f00\u66f4\u65e9\u8bb0\u5f55\uff08${hiddenItems.length} \u6761\uff09`}
          </button>
          ${state.historyExpanded ? `<div class="stack-list history-hidden">${hiddenItems.map(renderHistoryCard).join("")}</div>` : ""}
        </section>
      `
      : ""}
  `;
}

function renderEvidencePrompt(mode = "before-ask") {
  const prompt = mode === "before-ask"
    ? {
        list: "请先在问数工作台完成提问，再来查看对应的 PDF 证据文件。",
        title: "请先提问后预览 PDF 证据",
        meta: "证据预览只展示当前问题命中的 PDF 文件，不展示普通上传文件或图表产物。",
        body: "先执行一次问答，系统才会把命中的 PDF 证据文件和对应答案片段整理到这里。",
      }
    : {
        list: "当前问题没有命中可预览的 PDF 证据文件。",
        title: "当前问题暂无 PDF 证据",
        meta: "可以换一个问题，或回到回答结果查看是否返回了 PDF 引用。",
        body: "如果回答依赖的是结构化数据库或非 PDF 引用，这里不会生成 PDF 证据预览。",
      };

  refs.evidenceList.innerHTML = `<div class="empty-box small">${escapeHtml(prompt.list)}</div>`;
  refs.previewTitle.textContent = prompt.title;
  refs.previewMeta.textContent = prompt.meta;
  refs.previewBody.innerHTML = `
    <div class="tip-box evidence-tip">
      <strong>${escapeHtml(prompt.title)}</strong>
      <p>${escapeHtml(prompt.body)}</p>
    </div>
  `;
}

function buildEvidenceEntries() {
  const groups = new Map();
  (state.latestAnswers || []).forEach((answer) => {
    const question = answer.q || state.currentQuestion || "当前问题";
    (answer.a?.references || []).forEach((ref) => {
      const path = String(ref.paper_path || "").trim();
      const text = String(ref.text || "").trim();
      if (!isPdfReference(path)) {
        return;
      }
      if (!groups.has(path)) {
        groups.set(path, {
          ref: path,
          label: basename(path),
          path,
          snippets: [],
          _signatures: new Set(),
        });
      }
      const entry = groups.get(path);
      const signature = `${question}::${text}`;
      if (entry._signatures.has(signature)) {
        return;
      }
      entry._signatures.add(signature);
      entry.snippets.push({
        question,
        text: text || "该 PDF 被用于回答，但当前未返回更细的证据片段。",
      });
    });
  });
  return [...groups.values()]
    .map(({ _signatures, ...item }) => item)
    .sort((left, right) => (right.snippets.length - left.snippets.length) || left.label.localeCompare(right.label, "zh-CN"));
}

function renderEvidencePreview(entry) {
  refs.previewTitle.textContent = entry.label;
  refs.previewMeta.innerHTML = `${escapeHtml(entry.path)} · 命中 ${entry.snippets.length} 条答案依据`;
  refs.previewBody.innerHTML = `
    <div class="tip-box evidence-tip">
      <strong>当前问题命中的 PDF 证据</strong>
      <p>下列内容直接来自本次问答返回的引用片段，用于说明该答案在 PDF 文件中的依据。</p>
    </div>
    <a class="inline-link" href="/api/download?path=${encodeURIComponent(entry.path)}" target="_blank" rel="noopener">下载该 PDF 文件</a>
    ${entry.snippets
      .map(
        (snippet, index) => `
          <article class="snippet-card">
            <div class="snippet-head">
              <span class="snippet-index">${index + 1}</span>
              <div>
                <strong>${escapeHtml(snippet.question)}</strong>
                <small>对应答案依据片段</small>
              </div>
            </div>
            <pre class="preview-text">${escapeHtml(snippet.text)}</pre>
          </article>
        `,
      )
      .join("")}
  `;
}

function openEvidencePreview(refValue) {
  if (!state.latestAnswers.length) {
    renderEvidencePrompt("before-ask");
    return;
  }
  const entry = state.evidenceEntries.find((item) => item.ref === refValue);
  if (!entry) {
    renderEvidencePrompt("no-pdf");
    return;
  }
  renderEvidencePreview(entry);
}

function refreshEvidenceEntries() {
  if (!state.latestAnswers.length) {
    state.evidenceEntries = [];
    renderEvidencePrompt("before-ask");
    return;
  }

  state.evidenceEntries = buildEvidenceEntries();
  if (!state.evidenceEntries.length) {
    renderEvidencePrompt("no-pdf");
    return;
  }

  refs.evidenceList.innerHTML = state.evidenceEntries
    .map(
      (item) => `
        <button class="evidence-card" data-evidence-ref="${escapeHtml(item.ref)}">
          <strong>${escapeHtml(item.label)}</strong>
          <span class="evidence-path">${escapeHtml(item.path)}</span>
          <small>命中 ${escapeHtml(item.snippets.length)} 条答案依据</small>
        </button>
      `,
    )
    .join("");

  renderEvidencePreview(state.evidenceEntries[0]);
}

async function askQuestion() {
  const question = refs.questionInput.value.trim();
  if (!question) {
    setStatus(refs.askStatus, "请输入问题", true);
    return;
  }
  state.currentQuestion = question;
  await withBusy(
    "ask",
    {
      buttons: [refs.askButton],
      statusNode: refs.askStatus,
      loadingText: "正在提问…",
    },
    async () => {
      const data = await api("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
      });
      if (data.error) {
        setStatus(refs.askStatus, responseMessage(data), true);
        return;
      }
      state.latestAnswers = data.answers || [];
      renderResults();
      setStatus(refs.askStatus, "已完成");
      await loadHistory();
      refreshEvidenceEntries();
    },
  );
}

async function loadHistory() {
  const data = await api("/api/answer-history");
  state.history = data.history || [];
  state.historyExpanded = false;
  renderHistory();
}

async function clearHistory() {
  await api("/api/answer-history/clear", { method: "POST" });
  await loadHistory();
}

function renderFiles() {
  const sections = [
    ["manual_source", "补充源文件"],
    ["manual_import", "结构化 CSV"],
    ["exports", "导出结果"],
    ["artifacts", "图表产物"],
  ];
  refs.fileSections.innerHTML = sections
    .map(([key, title]) => {
      const items = state.files?.[key] || [];
      return `
        <section class="file-section">
          <div class="section-title">${escapeHtml(title)}</div>
          ${items.length
            ? items
                .map(
                  (item) => `
                    <article class="file-card">
                      <div>
                        <strong>${escapeHtml(item.name)}</strong>
                        <p>${escapeHtml(item.path)}</p>
                        <small>${escapeHtml(item.size_text)} · ${escapeHtml(item.updated_at)}</small>
                      </div>
                      <div class="inline-actions">
                        ${item.downloadable ? `<button class="mini-button" data-download-file="${escapeHtml(item.path)}">下载</button>` : ""}
                      </div>
                    </article>
                  `,
                )
                .join("")
            : '<div class="empty-box small">暂无文件。</div>'}
        </section>
      `;
    })
    .join("");
}

async function loadFiles() {
  const data = await api("/api/files");
  state.files = data;
  renderFiles();
}

function getQuestionItems() {
  return [
    ...(state.systemQuestions || []).map((item) => ({ ...item, sourceKey: "system", scopeLabel: item.scope || "\u7cfb\u7edf\u751f\u6210" })),
    ...(state.officialQuestions || []).map((item) => ({ ...item, sourceKey: "official", scopeLabel: item.scope || "\u5b98\u65b9\u9898\u5e93" })),
    ...(state.customQuestions || []).map((item) => ({ ...item, sourceKey: "custom", scopeLabel: item.scope || "\u81ea\u5b9a\u4e49\u95ee\u9898" })),
  ];
}

function questionScopeClass(sourceKey) {
  if (sourceKey === "system") return "system";
  if (sourceKey === "custom") return "custom";
  return "official";
}

function getQuestionKey(item) {
  return `${item.sourceKey}::${item.id}`;
}

function isQuestionEditable(item) {
  return item.sourceKey === "system" || item.sourceKey === "custom";
}

function syncQuestionSelection(allItems) {
  const validKeys = new Set(allItems.filter((item) => isQuestionEditable(item)).map((item) => getQuestionKey(item)));
  state.selectedQuestionKeys = (state.selectedQuestionKeys || []).filter((key) => validKeys.has(key));
}

function compareQuestionTypes(left, right) {
  const leftIndex = QUESTION_TYPE_ORDER.indexOf(left);
  const rightIndex = QUESTION_TYPE_ORDER.indexOf(right);
  if (leftIndex !== -1 || rightIndex !== -1) {
    return (leftIndex === -1 ? 999 : leftIndex) - (rightIndex === -1 ? 999 : rightIndex);
  }
  return String(left || "").localeCompare(String(right || ""), "zh-CN");
}

function renderQuestionTypeFilter(allItems) {
  const types = [...new Set(allItems.map((item) => item.question_type).filter(Boolean))].sort(compareQuestionTypes);
  refs.questionTypeFilter.innerHTML = [
    '<option value="all">\u5168\u90e8\u7c7b\u578b</option>',
    ...types.map((type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`),
  ].join("");
  if (state.questionTypeFilter !== "all" && !types.includes(state.questionTypeFilter)) {
    state.questionTypeFilter = "all";
  }
  refs.questionTypeFilter.value = state.questionTypeFilter;
  refs.questionSourceFilter.value = state.questionSourceFilter;
}

function filterQuestionItems(allItems) {
  const keyword = refs.questionSearch.value.trim().toLowerCase();
  return allItems.filter((item) => {
    const tags = Array.isArray(item.tags) ? item.tags : [];
    if (state.activeQuestionTag !== "\u5168\u90e8" && !tags.includes(state.activeQuestionTag)) {
      return false;
    }
    if (state.questionSourceFilter !== "all" && item.sourceKey !== state.questionSourceFilter) {
      return false;
    }
    if (state.questionTypeFilter !== "all" && item.question_type !== state.questionTypeFilter) {
      return false;
    }
    if (!keyword) {
      return true;
    }
    return `${item.display || ""} ${item.question || item.question_payload || ""} ${item.title || ""} ${tags.join(" ")} ${item.question_type || ""} ${item.scopeLabel || ""}`.toLowerCase().includes(keyword);
  });
}

function renderQuestionOverview(allItems, visibleItems) {
  const sourceCounts = {
    system: allItems.filter((item) => item.sourceKey === "system").length,
    official: allItems.filter((item) => item.sourceKey === "official").length,
    custom: allItems.filter((item) => item.sourceKey === "custom").length,
  };
  const selectedCount = (state.selectedQuestionKeys || []).length;
  const sourceLabel = state.questionSourceFilter === "all"
    ? "全部来源"
    : (state.questionSourceFilter === "system" ? "系统生成" : state.questionSourceFilter === "official" ? "官方题库" : "自定义问题");
  refs.questionOverview.innerHTML = `
    <article class="question-overview-card">
      <span>\u9898\u5e93\u603b\u91cf</span>
      <strong>${allItems.length}</strong>
      <small>\u7cfb\u7edf\u751f\u6210 ${sourceCounts.system} / \u5b98\u65b9 ${sourceCounts.official} / \u81ea\u5b9a\u4e49 ${sourceCounts.custom}</small>
    </article>
    <article class="question-overview-card">
      <span>\u5f53\u524d\u7b5b\u9009</span>
      <strong>${visibleItems.length}</strong>
      <small>\u6765\u6e90：${sourceLabel}</small>
    </article>
    <article class="question-overview-card">
      <span>\u5f53\u524d\u7c7b\u578b</span>
      <strong>${escapeHtml(state.questionTypeFilter === "all" ? "\u5168\u90e8" : state.questionTypeFilter)}</strong>
      <small>\u6807\u7b7e：${escapeHtml(state.activeQuestionTag)}</small>
    </article>
    <article class="question-overview-card emphasis">
      <span>\u5df2\u9009\u95ee\u9898</span>
      <strong>${selectedCount}</strong>
      <small>\u4ec5\u652f\u6301\u6279\u91cf\u5220\u9664\u7cfb\u7edf\u751f\u6210\u548c\u81ea\u5b9a\u4e49\u95ee\u9898</small>
    </article>
  `;
}

function renderQuestionTags(totalCount, visibleCount) {
  const tags = state.questionFilterTags || [];
  refs.questionTagBar.innerHTML = [
    `<button class="question-filter-chip ${state.activeQuestionTag === "\u5168\u90e8" ? "active" : ""}" data-filter-tag="\u5168\u90e8">\u5168\u90e8 <em>${totalCount}</em></button>`,
    ...tags.map((item) => `<button class="question-filter-chip ${state.activeQuestionTag === item.name ? "active" : ""}" data-filter-tag="${escapeHtml(item.name)}">${escapeHtml(item.name)} <em>${escapeHtml(item.count)}</em></button>`),
  ].join("");
  const sourceLabel = state.questionSourceFilter === "all" ? "\u5168\u90e8\u6765\u6e90" : (state.questionSourceFilter === "system" ? "\u7cfb\u7edf\u751f\u6210" : state.questionSourceFilter === "official" ? "\u5b98\u65b9\u9898\u5e93" : "\u81ea\u5b9a\u4e49\u95ee\u9898");
  const typeLabel = state.questionTypeFilter === "all" ? "\u5168\u90e8\u7c7b\u578b" : state.questionTypeFilter;
  refs.questionTagSummary.textContent = `当前标签：${state.activeQuestionTag} · ${sourceLabel} · ${typeLabel} · 显示 ${visibleCount} / ${totalCount} 题`;
}

function renderQuestionLibrary() {
  const allItems = getQuestionItems();
  refs.questionPanelTitle.textContent = `问题管理（${allItems.length}）`;
  syncQuestionSelection(allItems);
  renderQuestionTypeFilter(allItems);
  const items = filterQuestionItems(allItems);
  const selectedKeys = new Set(state.selectedQuestionKeys || []);
  const editableVisibleItems = items.filter((item) => isQuestionEditable(item));
  const selectedVisibleCount = editableVisibleItems.filter((item) => selectedKeys.has(getQuestionKey(item))).length;
  const isAllVisibleSelected = editableVisibleItems.length > 0 && selectedVisibleCount === editableVisibleItems.length;

  refs.deleteSelectedQuestionsButton.disabled = !state.selectedQuestionKeys.length;
  refs.deleteSelectedQuestionsButton.textContent = state.selectedQuestionKeys.length
    ? `删除所选（${state.selectedQuestionKeys.length}）`
    : "\u5220\u9664\u6240\u9009";

  renderQuestionOverview(allItems, items);
  renderQuestionTags(allItems.length, items.length);

  refs.questionLibrary.innerHTML = items.length
    ? `
        <div class="question-table-wrap">
          <table class="question-manage-table">
            <thead>
              <tr>
                <th class="question-check-col">
                  <label class="question-check-all">
                    <input type="checkbox" data-select-all-questions ${isAllVisibleSelected ? "checked" : ""} ${editableVisibleItems.length ? "" : "disabled"} />
                    <span>全选</span>
                  </label>
                </th>
                <th>\u95ee\u9898</th>
                <th>\u6807\u7b7e</th>
                <th>\u6765\u6e90</th>
                <th>\u7c7b\u578b</th>
                <th class="question-actions-col">\u64cd\u4f5c</th>
              </tr>
            </thead>
            <tbody>
              ${items.map((item) => {
                  const questionKey = getQuestionKey(item);
                  const editable = isQuestionEditable(item);
                  const checked = selectedKeys.has(questionKey);
                  const tags = Array.isArray(item.tags) ? item.tags : [];
                  const visibleTags = tags.slice(0, 3);
                  const extraTags = Math.max(0, tags.length - visibleTags.length);
                  const sourceHint = item.sourceKey === "system"
                    ? "\u7cfb\u7edf\u6839\u636e\u5f53\u524d\u6570\u636e\u52a8\u6001\u751f\u6210"
                    : item.sourceKey === "official"
                      ? (item.source_file || "\u5b98\u65b9\u9898\u5e93\u6765\u6e90")
                      : (item.note || "\u81ea\u5b9a\u4e49\u7ef4\u62a4\u95ee\u9898");
                  return `
                    <tr class="${checked ? "selected" : ""}">
                      <td class="question-check-cell"><input type="checkbox" data-select-question="${escapeHtml(questionKey)}" ${editable ? "" : "disabled"} ${checked ? "checked" : ""} /></td>
                      <td class="question-main-cell">
                        <div class="question-title-block">
                          <strong>${escapeHtml(item.title || item.display || "\u672a\u547d\u540d\u95ee\u9898")}</strong>
                          <p>${escapeHtml(item.display || item.question || item.question_payload || "")}</p>
                          <small>${escapeHtml([item.question_id, item.note].filter(Boolean).join(" · ") || "\u53ef\u76f4\u63a5\u56de\u586b\u5230\u95ee\u6570\u5de5\u4f5c\u53f0")}</small>
                        </div>
                      </td>
                      <td>
                        <div class="question-tag-stack">
                          ${visibleTags.map((tag) => `<button class="tag-chip compact ${state.activeQuestionTag === tag ? "active" : ""}" data-filter-tag="${escapeHtml(tag)}">${escapeHtml(tag)}</button>`).join("")}
                          ${extraTags ? `<span class="tag-chip compact ghost">+${extraTags}</span>` : ""}
                        </div>
                      </td>
                      <td>
                        <div class="question-source-block">
                          <span class="type-badge ${questionScopeClass(item.sourceKey)}">${escapeHtml(item.scopeLabel)}</span>
                          <small>${escapeHtml(sourceHint)}</small>
                        </div>
                      </td>
                      <td><span class="type-badge type-tag standalone">${escapeHtml(item.question_type || "\u672a\u5206\u7c7b")}</span></td>
                      <td>
                        <div class="question-actions">
                          <button class="table-action-button" data-use-question="${escapeHtml(questionKey)}">\u4f7f\u7528</button>
                          ${item.sourceKey === "custom" ? `<button class="table-action-button" data-edit-question="${escapeHtml(item.id)}">\u7f16\u8f91</button>` : ""}
                          ${editable ? `<button class="table-action-button danger" data-delete-question="${escapeHtml(questionKey)}">\u5220\u9664</button>` : ""}
                        </div>
                      </td>
                    </tr>
                  `;
                }).join("")}
            </tbody>
          </table>
        </div>
      `
    : '<div class="empty-box question-empty">\u5f53\u524d\u7b5b\u9009\u6761\u4ef6\u4e0b\u6ca1\u6709\u5339\u914d\u7684\u95ee\u9898\u3002</div>';
}

async function deleteQuestionKeys(questionKeys) {
  const keys = (questionKeys || []).filter(Boolean);
  if (!keys.length) {
    return;
  }
  const normalized = [...new Set(keys)];
  if (!window.confirm(`\u5c06\u5220\u9664 ${normalized.length} \u4e2a\u95ee\u9898\uff0c\u662f\u5426\u7ee7\u7eed\uff1f`)) {
    return;
  }
  setStatus(refs.questionStatus, "\u5904\u7406\u4e2d...");
  const editingKey = state.editingQuestionId ? `custom::${state.editingQuestionId}` : "";
  const results = await Promise.all(
    normalized.map((key) => {
      const [scope, id] = (key || "::").split("::");
      const endpoint = scope === "system" ? "/api/system-questions/delete" : "/api/custom-questions/delete";
      return api(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id }),
      });
    }),
  );
  const failedCount = results.filter((row) => row?.error || row?.ok === false).length;
  if (editingKey && normalized.includes(editingKey)) {
    resetQuestionForm();
  }
  state.selectedQuestionKeys = (state.selectedQuestionKeys || []).filter((key) => !normalized.includes(key));
  await loadQuestions();
  if (failedCount) {
    setStatus(refs.questionStatus, `\u5df2\u5904\u7406 ${normalized.length - failedCount} \u4e2a\u95ee\u9898\uff0c${failedCount} \u4e2a\u5904\u7406\u5931\u8d25`, true);
    return;
  }
  setStatus(refs.questionStatus, `\u5df2\u5904\u7406 ${normalized.length} \u4e2a\u95ee\u9898`);
}

async function loadQuestions() {
  const data = await api("/api/question-bank");
  state.officialQuestions = data.official || [];
  state.systemQuestions = data.system || [];
  state.customQuestions = data.custom || [];
  state.questionFilterTags = data.tags || [];
  const allTags = new Set((state.questionFilterTags || []).map((item) => item.name));
  if (state.activeQuestionTag !== "\u5168\u90e8" && !allTags.has(state.activeQuestionTag)) {
    state.activeQuestionTag = "\u5168\u90e8";
  }
  renderQuestionLibrary();
}

async function loadTables() {
  const data = await api("/api/tables");
  state.tables = data.tables || [];
  refs.tableList.innerHTML = state.tables.length
    ? state.tables
        .map(
          (table) => `
            <button class="table-card ${state.selectedTable === table.name ? "active" : ""}" data-table-name="${escapeHtml(table.name)}">
              <strong>${escapeHtml(table.name)}</strong>
              <span>${escapeHtml(table.count)} 行 · ${escapeHtml(table.column_count)} 列</span>
            </button>
          `,
        )
        .join("")
    : '<div class="empty-box small">暂无可展示的数据表。</div>';
  if (!state.tables.length) {
    refs.tableTitle.textContent = "当前没有可预览的数据表";
    refs.tableMeta.textContent = "请先导入并构建数据库。";
    refs.tablePreview.innerHTML = "";
  }
}

async function loadTablePreview(tableName, page = 1) {
  if (!tableName) {
    return;
  }
  const data = await api(`/api/table-preview?name=${encodeURIComponent(tableName)}&page=${page}&page_size=12`);
  if (data.error) {
    refs.tableMeta.textContent = data.error;
    return;
  }
  state.selectedTable = tableName;
  state.tablePage = data.page || 1;
  state.tableTotal = data.total || 0;
  refs.tableTitle.textContent = data.name || tableName;
  refs.tableMeta.textContent = `第 ${state.tablePage} 页 · 共 ${state.tableTotal} 行`;
  refs.tablePreview.innerHTML = `
    <thead><tr>${(data.columns || []).map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>
    <tbody>
      ${(data.rows || []).map((row) => `<tr>${(data.columns || []).map((column) => `<td>${escapeHtml(row[column] || "")}</td>`).join("")}</tr>`).join("")}
    </tbody>
  `;
  await loadTables();
}

async function ensureDefaultTablePreview() {
  if (!state.tables.length) {
    return;
  }
  const hasSelected = state.selectedTable && state.tables.some((table) => table.name === state.selectedTable);
  const targetTable = hasSelected ? state.selectedTable : state.tables[0].name;
  const targetPage = hasSelected ? Math.max(1, state.tablePage || 1) : 1;
  await loadTablePreview(targetTable, targetPage);
}

function resetQuestionForm() {
  state.editingQuestionId = "";
  refs.questionTitle.value = "";
  refs.questionPayload.value = "";
  refs.questionTags.value = "";
  refs.questionNote.value = "";
  setStatus(refs.questionStatus, "");
}

async function saveQuestion() {
  const question = refs.questionPayload.value.trim();
  if (!question) {
    setStatus(refs.questionStatus, "请输入问题内容", true);
    refs.questionPayload.focus();
    return;
  }
  const payload = {
    id: state.editingQuestionId || "",
    title: refs.questionTitle.value.trim(),
    question,
    tags: refs.questionTags.value.trim(),
    note: refs.questionNote.value.trim(),
  };
  setStatus(refs.questionStatus, "保存中...");
  const data = await api("/api/custom-questions/save", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (data.error) {
    setStatus(refs.questionStatus, data.error, true);
    return;
  }
  const actionText = state.editingQuestionId ? "自定义问题已更新" : "自定义问题已保存";
  resetQuestionForm();
  await loadQuestions();
  setStatus(refs.questionStatus, actionText);
}

async function uploadFile() {
  const file = refs.uploadFile.files?.[0];
  if (!file) {
    setStatus(refs.uploadStatus, "请选择文件", true);
    return;
  }
  const formData = new FormData();
  formData.append("category", refs.uploadCategory.value);
  formData.append("file", file);
  await withBusy(
    "upload",
    {
      buttons: [refs.uploadButton],
      statusNode: refs.uploadStatus,
      loadingText: "正在上传…",
    },
    async () => {
      const data = await api("/api/upload", { method: "POST", body: formData });
      if (data.error) {
        setStatus(refs.uploadStatus, responseMessage(data, "上传失败"), true);
        return;
      }
      setStatus(refs.uploadStatus, `已上传：${data.file?.name || file.name}`);
      refs.uploadFile.value = "";
      await Promise.all([loadFiles(), loadOverview()]);
    },
  );
}

async function rebuildDatabase() {
  await withBusy(
    "rebuild",
    {
      buttons: [refs.rebuildButton, refs.rebuildTopButton, refs.askButton, refs.uploadButton],
      statusNode: refs.uploadStatus,
      loadingText: "正在重建数据库，请稍候…",
    },
    async () => {
      state.databaseRebuilding = true;
      syncActionAvailability();
      setStatus(refs.askStatus, "正在重建数据库，请稍候…", true);
      const data = await api("/api/rebuild-database", { method: "POST" });
      if (data.error) {
        setStatus(refs.uploadStatus, responseMessage(data), true);
        await loadOverview();
        return;
      }
      setStatus(refs.uploadStatus, responseMessage(data, "数据库已重建"));
      setStatus(refs.askStatus, "");
      await Promise.all([loadOverview(), loadFiles(), loadQuestions(), loadTables()]);
      await ensureDefaultTablePreview();
    },
  );
  state.databaseRebuilding = false;
  syncActionAvailability();
}

function bindEvents() {
  refs.navItems.forEach((item) => item.addEventListener("click", () => setView(item.dataset.view)));
  refs.askButton.addEventListener("click", askQuestion);
  refs.loadSamplesButton.addEventListener("click", loadSamples);
  refs.resetContextButton.addEventListener("click", async () => {
    await api("/api/reset-context");
    state.latestAnswers = [];
    renderResults();
    refreshEvidenceEntries();
    setStatus(refs.askStatus, "上下文已清空");
  });
  refs.exportLatestButton.addEventListener("click", () => exportRecords(buildExportRecordsFromAnswers()));
  refs.exportHistoryButton.addEventListener("click", () => exportRecords(buildExportRecordsFromHistory()));
  refs.clearHistoryButton.addEventListener("click", clearHistory);
  refs.uploadButton.addEventListener("click", uploadFile);
  refs.rebuildButton.addEventListener("click", rebuildDatabase);
  refs.rebuildTopButton.addEventListener("click", rebuildDatabase);
  refs.saveQuestionButton.addEventListener("click", saveQuestion);
  refs.resetQuestionButton.addEventListener("click", resetQuestionForm);
  refs.newQuestionButton.addEventListener("click", () => {
    resetQuestionForm();
    refs.questionTitle.focus();
    refs.questionTitle.scrollIntoView({ behavior: "smooth", block: "center" });
  });
  refs.clearQuestionFiltersButton.addEventListener("click", () => {
    refs.questionSearch.value = "";
    state.activeQuestionTag = "全部";
    state.questionSourceFilter = "all";
    state.questionTypeFilter = "all";
    state.selectedQuestionKeys = [];
    renderQuestionLibrary();
  });
  refs.deleteSelectedQuestionsButton.addEventListener("click", () => deleteQuestionKeys(state.selectedQuestionKeys));
  refs.questionSearch.addEventListener("input", renderQuestionLibrary);
  refs.questionSourceFilter.addEventListener("change", (event) => {
    state.questionSourceFilter = event.target.value || "all";
    renderQuestionLibrary();
  });
  refs.questionTypeFilter.addEventListener("change", (event) => {
    state.questionTypeFilter = event.target.value || "all";
    renderQuestionLibrary();
  });
  refs.questionTagBar.addEventListener("click", (event) => {
    const target = event.target.closest("[data-filter-tag]");
    if (!target) return;
    state.activeQuestionTag = target.dataset.filterTag || "全部";
    renderQuestionLibrary();
  });
  refs.prevTablePageButton.addEventListener("click", () => loadTablePreview(state.selectedTable, Math.max(1, state.tablePage - 1)));
  refs.nextTablePageButton.addEventListener("click", () => loadTablePreview(state.selectedTable, state.tablePage + 1));

  refs.samplesBox.addEventListener("click", (event) => {
    const target = event.target.closest("[data-sample]");
    if (!target) return;
    refs.questionInput.value = target.dataset.sample || "";
  });
  refs.resultsBox.addEventListener("click", (event) => {
    const target = event.target.closest("[data-ref]");
    if (!target) return;
    setView("evidence");
    openEvidencePreview(target.dataset.ref);
  });
  refs.historyList.addEventListener("click", (event) => {
    const toggleTarget = event.target.closest("[data-history-toggle]");
    if (toggleTarget) {
      state.historyExpanded = toggleTarget.dataset.historyToggle === "expand";
      renderHistory();
      return;
    }
    const target = event.target.closest("[data-history-use]");
    if (!target) return;
    const item = state.history.find((row) => row.id === target.dataset.historyUse);
    if (item) {
      refs.questionInput.value = item.raw_question || "";
      setView("workspace");
    }
  });
  refs.fileSections.addEventListener("click", (event) => {
    const downloadTarget = event.target.closest("[data-download-file]");
    if (downloadTarget) {
      window.open(`/api/download?path=${encodeURIComponent(downloadTarget.dataset.downloadFile)}`, "_blank", "noopener");
    }
  });
  refs.questionLibrary.addEventListener("change", (event) => {
    const selectAllTarget = event.target.closest("[data-select-all-questions]");
    if (selectAllTarget) {
      const visibleEditableKeys = filterQuestionItems(getQuestionItems())
        .filter((item) => isQuestionEditable(item))
        .map((item) => getQuestionKey(item));
      const selected = new Set(state.selectedQuestionKeys || []);
      if (selectAllTarget.checked) {
        visibleEditableKeys.forEach((key) => selected.add(key));
      } else {
        visibleEditableKeys.forEach((key) => selected.delete(key));
      }
      state.selectedQuestionKeys = [...selected];
      renderQuestionLibrary();
      return;
    }
    const rowSelectTarget = event.target.closest("[data-select-question]");
    if (rowSelectTarget) {
      const selected = new Set(state.selectedQuestionKeys || []);
      if (rowSelectTarget.checked) {
        selected.add(rowSelectTarget.dataset.selectQuestion);
      } else {
        selected.delete(rowSelectTarget.dataset.selectQuestion);
      }
      state.selectedQuestionKeys = [...selected];
      renderQuestionLibrary();
    }
  });
  refs.questionLibrary.addEventListener("click", (event) => {
    const filterTarget = event.target.closest("[data-filter-tag]");
    if (filterTarget) {
      state.activeQuestionTag = filterTarget.dataset.filterTag || "全部";
      renderQuestionLibrary();
      return;
    }
    const useTarget = event.target.closest("[data-use-question]");
    if (useTarget) {
      const [scope, id] = (useTarget.dataset.useQuestion || "::").split("::");
      const sourceMap = {
        system: state.systemQuestions,
        official: state.officialQuestions,
        custom: state.customQuestions,
      };
      const source = sourceMap[scope] || [];
      const item = source.find((row) => String(row.id) === id);
      if (item) {
        refs.questionInput.value = item.question || item.question_payload || "";
        setView("workspace");
      }
      return;
    }
    const editTarget = event.target.closest("[data-edit-question]");
    if (editTarget) {
      const item = state.customQuestions.find((row) => String(row.id) === editTarget.dataset.editQuestion);
      if (item) {
        state.editingQuestionId = item.id;
        refs.questionTitle.value = item.title || "";
        refs.questionPayload.value = item.question || "";
        refs.questionTags.value = (item.user_tags || (item.tags || []).filter((tag) => tag !== "自定义问题")).join(", ");
        refs.questionNote.value = item.note || "";
        setStatus(refs.questionStatus, "已载入当前问题，可直接修改后保存");
        refs.questionTitle.scrollIntoView({ behavior: "smooth", block: "center" });
      }
      return;
    }
    const deleteTarget = event.target.closest("[data-delete-question]");
    if (deleteTarget) {
      deleteQuestionKeys([deleteTarget.dataset.deleteQuestion]);
    }
  });
  refs.tableList.addEventListener("click", (event) => {
    const target = event.target.closest("[data-table-name]");
    if (!target) return;
    loadTablePreview(target.dataset.tableName, 1);
  });
  refs.evidenceList.addEventListener("click", (event) => {
    const target = event.target.closest("[data-evidence-ref]");
    if (!target) return;
    openEvidencePreview(target.dataset.evidenceRef);
  });
}

async function boot() {
  try {
    bindEvents();
    await Promise.all([loadOverview(), loadSamples(), loadHistory(), loadQuestions(), loadFiles(), loadTables()]);
    await ensureDefaultTablePreview();
    renderResults();
    refreshEvidenceEntries();
  } catch (error) {
    console.error("Web UI boot failed", error);
    setStatus(refs.askStatus, "前端初始化失败，请刷新页面", true);
  }
}

boot();
