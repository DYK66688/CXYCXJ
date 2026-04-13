# 上市公司财报“智能问数”助手

这是一个可直接落地的竞赛基线项目，面向当前目录里的题目 PDF 和示例数据设计。项目目标不是做一个“只能展示页面的空壳”，而是提供完整的离线工程链路：

- 自动发现题目附件和示例数据
- 读取 `xlsx` 并构建 SQLite 数据库
- 创建财务表结构、公司信息表、题库表、研报表
- 对 PDF 做轻量文本抽取并建立检索索引
- 用规则完成实体识别、意图识别、SQL 模板生成、多意图上下文承接
- 支持本地 Web 问答界面
- 支持批量导出题库答案到 `xlsx`

## 项目结构

```text
.
├─ run.py
├─ README.md
├─ requirements.txt
├─ src/financial_qa_assistant/
│  ├─ assistant.py
│  ├─ charting.py
│  ├─ cli.py
│  ├─ config.py
│  ├─ database.py
│  ├─ pdf_tools.py
│  ├─ utils.py
│  ├─ web.py
│  └─ xlsx_tools.py
├─ web/static/
│  ├─ app.js
│  ├─ index.html
│  └─ styles.css
└─ build/
   ├─ artifacts/
   └─ manual_import/
```

## 快速开始

### 1. 建库与索引

```powershell
python run.py ingest
```

执行后会在 `build/finance_qa_assistant.sqlite3` 生成数据库。

### 2. 单次提问

```powershell
python run.py ask "华润三九的员工人数是多少"
python run.py ask "[{\"Q\":\"金花股份利润总额是多少\"},{\"Q\":\"2025年第三季度的\"}]"
```

### 3. 启动本地 Web 页面

```powershell
python run.py serve --host 127.0.0.1 --port 8000
```

浏览器打开 `http://127.0.0.1:8000`。

### 4. 批量导出题库答案

```powershell
python run.py export
```

导出文件会落到 `build/artifacts/`。

## 已实现能力

### 结构化数据

- 自动读取公司基础信息、题库、个股研报、行业研报元数据
- 从“数据库-表名及字段说明”自动创建财务相关表
- 支持从 `build/manual_import/*.csv` 导入结构化财务结果

### 问答能力

- 公司信息查询
- 研报列表查询
- 多意图上下文承接
- 趋势/TopK 查询的 SQL 模板生成
- PDF/元数据检索兜底
- 原因类问题的证据片段抽取

### 可视化

- 纯 SVG 生成折线图和柱状图
- 图像输出到 `build/artifacts/`

## 关于财务报表结构化数据

当前样例数据里，真正的财务数值主要还在 PDF 中。这个项目已经把两层能力拆开了：

1. `数据库建模 + SQL 问答`
2. `PDF/研报检索兜底`

因此：

- 如果财务表暂时没有导入数据，系统仍然会给出对应 SQL 模板和检索结果
- 如果你后续补充了 OCR/表格抽取结果，只需把对应 CSV 放到 `build/manual_import/`，再运行一次 `python run.py ingest`

CSV 文件名需要和英文表名一致，例如：

- `income_sheet.csv`
- `balance_sheet.csv`
- `cash_flow_sheet.csv`
- `core_performance_indicators_sheet.csv`

## 建议下一步增强

- 接入更强的 PDF 表格抽取或 OCR
- 将“原因归因”从规则摘要升级为大模型摘要
- 给 `income_sheet` 等表补充更多字段映射别名
- 增加评测脚本，对题库批量输出做一致性检查

## 说明

这个版本优先保证：

- 可运行
- 可扩展
- 竞赛工程结构完整

它不是最终的高分解法，但已经是一个足够扎实的基线项目，可以直接作为后续迭代的主工程。
