# 上市公司财报“智能问数”助手

这是一个面向泰迪杯赛题数据的本地化问答工程，目标不是只做演示页面，而是提供一套可以直接下载、建库、问答、导出结果的完整工作流。

项目核心能力：

- 自动发现赛题目录中的 Excel、PDF 与题库文件
- 将基础信息、题库、研报元数据和文档分片写入 SQLite
- 支持命令行单问、批量导出和本地 Web 界面
- 支持手工补充源文件与结构化 CSV 后重建数据库
- 支持打包生成干净交付包

## 仓库说明

这个 GitHub 仓库刻意没有提交 `题目/` 目录中的大体积原始赛题数据，因为其中包含多个超过 GitHub 普通推送限制的大文件。

这意味着：

- 你克隆仓库后，可以直接安装依赖并启动 Web 界面
- 如果没有正式赛题数据，`serve` 可以启动，但无法直接完成正式问答
- 想要完整建库和正式问答，需要把赛题数据补到工作区，或者通过 `build/manual_source/` 上传补充

如果你是另一个 Codex，优先按下面“最短启动路径”执行，不要先假设 `题目/` 目录已经存在。

## 最短启动路径

### 1. 克隆并进入项目

```powershell
git clone https://github.com/DYK66688/CXYCXJ.git
cd CXYCXJ
```

### 2. 安装依赖

推荐 Python `3.10+`。

```powershell
python -m pip install -r requirements.txt
```

当前依赖很轻：

- `pypdf`
- `Pillow`

### 3. 直接启动本地界面

```powershell
python run.py
```

不带参数时，程序默认等价于：

```powershell
python run.py serve --host 127.0.0.1 --port 8000
```

浏览器访问：

```text
http://127.0.0.1:8000
```

### 4. 如果你还没有赛题数据

这是 GitHub 克隆后的常见状态。此时建议：

- 先启动 Web 界面，确认程序和前端可以正常运行
- 将正式赛题的 Excel / PDF 放入 `build/manual_source/`
- 或者把完整正式数据目录放回项目下的 `题目/`
- 然后在页面点击“重建数据库”，或执行 `python run.py ingest`

## 完整复现路径

### 方案 A：你有官方正式数据目录

项目会优先查找这个相对路径：

```text
题目/B题数据及提交说明/全部数据/正式数据
```

至少需要能识别出这些关键文件：

- `附件1：中药上市公司基本信息（截至到2025年12月22日）.xlsx`
- `附件3：数据库-表名及字段说明.xlsx`
- `附件4：问题汇总.xlsx`
- `附件6：问题汇总.xlsx`

放好后直接执行：

```powershell
python run.py ingest
```

完成后数据库默认生成在：

```text
build/artifacts/finance_qa_assistant.sqlite3
```

然后启动：

```powershell
python run.py serve
```

### 方案 B：你只有代码仓库，没有 `题目/`

可以用这两种方式补数据：

1. 手工把官方 Excel / PDF 拷贝到 `build/manual_source/`
2. 启动 Web 界面后，通过上传功能导入到 `build/manual_source/`

之后执行：

```powershell
python run.py ingest
```

或者在 Web 页面点击“重建数据库”。

### 方案 C：你已经有额外结构化财务 CSV

将 CSV 放入：

```text
build/manual_import/
```

再执行：

```powershell
python run.py ingest
```

CSV 文件名需要和英文表名一致，例如：

- `income_sheet.csv`
- `balance_sheet.csv`
- `cash_flow_sheet.csv`
- `core_performance_indicators_sheet.csv`

## 命令行用法

### `python run.py ingest`

导入 Excel / PDF 并重建 SQLite 数据库。

适用场景：

- 第一次建库
- 替换了正式数据目录
- 上传了新的 `manual_source` 文件
- 补充了新的 `manual_import` CSV

### `python run.py ask "<问题>"`

单次提问，输出 JSON。

示例：

```powershell
python run.py ask "华润三九的员工人数是多少"
python run.py ask "列出与云南白药有关的研报"
```

也支持一次传入多问题 JSON 数组：

```powershell
python run.py ask "[{\"Q\":\"金花股份利润总额是多少\"},{\"Q\":\"2025年第三季度营业收入是多少\"}]"
```

### `python run.py serve --host 127.0.0.1 --port 8000`

启动本地 Web 服务。

特性包括：

- 问答
- 题库浏览
- 表预览
- 文件上传
- 结果导出
- 参考文件预览
- 历史记录查看

### `python run.py export`

批量导出题库答案。

默认会尝试处理自动发现到的“问题汇总”文件，并将结果写到 `提交文件/`。

也可手动指定输入与输出：

```powershell
python run.py export --question-file "某个问题汇总.xlsx" --output "提交文件/我的结果.xlsx"
```

默认命名规则：

- 文件名含 `附件4` 时导出为 `提交文件/result_2.xlsx`
- 文件名含 `附件6` 时导出为 `提交文件/result_3.xlsx`
- 其他文件按 `原文件名_答案结果.xlsx` 导出

### `python run.py demo --limit 20`

输出题库前若干条问题及当前系统生成的答案，适合快速检查系统是否可用。

### `python run.py package --name financial_qa_assistant`

生成一个干净交付包到：

```text
build/packages/
```

会自动排除这些目录或产物：

- `.git`
- `.idea`
- `.venv`
- `__pycache__`
- `.pytest_cache`
- `build`
- `result`
- `提交文件`

注意：如果你的工作区中存在 `题目/`，打包命令会把它包含进去，因为交付包通常需要携带赛题数据。

## Web 使用说明

启动 `serve` 后，Web 端适合以下流程：

1. 查看系统概览，确认数据库状态和数据源状态
2. 如果数据库未就绪，上传文件到 `manual_source` 或 `manual_import`
3. 点击“重建数据库”
4. 在问答框里直接提问
5. 查看回答中的 SQL、参考文件、图表和证据片段
6. 将当前问答记录导出为 `xlsx` 或 `json`

Web 会把运行时状态写到 `build/runtime/`，例如：

- `question_library.json`
- `answer_history.json`
- `system_question_state.json`
- `ingest_manifest.json`
- `ingest_report.json`

## 项目目录

```text
.
├─ run.py
├─ README.md
├─ REPRODUCE.md
├─ requirements.txt
├─ src/financial_qa_assistant/
│  ├─ assistant.py
│  ├─ bundle.py
│  ├─ charting.py
│  ├─ cli.py
│  ├─ config.py
│  ├─ database.py
│  ├─ database_base.py
│  ├─ database_extract.py
│  ├─ pdf_tools.py
│  ├─ question_bank.py
│  ├─ utils.py
│  ├─ web.py
│  └─ xlsx_tools.py
├─ scripts/
├─ seed_data/
├─ tests/
├─ web/static/
├─ build/
├─ result/
├─ 提交文件/
└─ 题目/
```

说明：

- `src/financial_qa_assistant/` 是主程序
- `web/static/` 是前端静态页面
- `seed_data/` 是仓库内自带的小型种子数据
- `build/` 是数据库、运行时状态、导出结果和临时文件目录
- `result/` 是图表等产物目录
- `提交文件/` 是批量导出的比赛提交结果
- `题目/` 是官方数据目录，GitHub 仓库默认不包含

## 关键输出目录

建库和运行后，常见产物位置如下：

- 数据库：`build/artifacts/finance_qa_assistant.sqlite3`
- 上一次数据库备份：`build/artifacts/finance_qa_assistant.previous.sqlite3`
- 运行状态：`build/runtime/`
- Web 导出结果：`build/exports/`
- 打包文件：`build/packages/`
- 图表和图片产物：`result/`
- 题库批量导出：`提交文件/`

## 数据发现规则

程序不会只死盯一个目录，而是会按配置自动搜索：

1. 优先查找 `build/manual_source/`
2. 其次查找 `题目/B题数据及提交说明/全部数据/正式数据`
3. 如果标准路径不存在，会在工作区内递归寻找名为“正式数据”的目录

识别时会自动跳过这些目录：

- `build`
- `result`
- `.venv`
- `.idea`

这意味着你不必强行把数据文件写死在某个路径，只要文件名和内容符合赛题目录惯例，系统就能较稳定地发现。

## 给其他 Codex 的建议执行顺序

如果你是下载这个仓库后准备继续开发的 Codex，建议按下面顺序做，不要一上来先改代码：

1. `python -m pip install -r requirements.txt`
2. `python run.py --help`
3. `python run.py serve`
4. 检查 `http://127.0.0.1:8000` 是否能打开
5. 查看页面中的数据库状态
6. 如果缺正式数据，补到 `题目/` 或 `build/manual_source/`
7. 执行 `python run.py ingest`
8. 再做提问、导出或功能开发

如果只是想确认代码能启动，而不是马上拿正式数据跑全流程，那么只做到第 4 步就够了。

## 常见问题

### 1. `serve` 能启动，但提问时报数据库未就绪

这是正常情况，说明你当前只有代码仓库，还没有可建库的数据源。

处理方式：

- 补充正式赛题数据到 `题目/`
- 或上传 Excel / PDF 到 `build/manual_source/`
- 然后执行 `python run.py ingest` 或在 Web 端点击“重建数据库”

### 2. `ingest` 报找不到 Excel

通常说明缺少以下文件之一：

- 公司基本信息 Excel
- 字段说明 Excel
- 问题汇总 Excel

先检查你的目录结构和文件名是否接近官方原始命名。

### 3. GitHub 克隆后为什么没有 `题目/`

因为官方数据体积太大，且包含超过 GitHub 普通推送限制的文件，所以仓库只保留代码和小型种子数据，不直接提交完整赛题包。

### 4. 图表依赖什么环境

项目优先使用 `Pillow` 生成图表图片，跨平台可用；当前 `requirements.txt` 已包含它。只要正常安装依赖，通常不需要额外处理图表环境。

## 当前版本的定位

这个版本优先保证三件事：

- 可运行
- 可扩展
- 可交付

它不是最终的高分方案，但已经具备一套完整、清晰、适合继续迭代的工程骨架。对另一个 Codex 来说，仓库拉下来后不需要先猜路径、猜入口、猜输出目录，按本 README 即可直接接手。
