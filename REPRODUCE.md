# 复现与交付说明

## 1. 安装依赖

```powershell
python -m pip install -r requirements.txt
```

## 2. 构建数据库

```powershell
python run.py ingest
```

默认会从正式赛题数据目录构建数据库：

- `题目/B题数据及提交说明/全部数据/正式数据`

生成结果：`build/artifacts/finance_qa_assistant.sqlite3`

如果正式数据目录发生变更，系统在下次启动时也会自动检测并重新建库。

## 3. 启动本地系统

```powershell
python run.py serve --host 127.0.0.1 --port 8000
```

访问：`http://127.0.0.1:8000`

## 4. 手工导入数据

Web 端支持两类导入：

- `manual_source`：补充 Excel / PDF / CSV / JSON / TXT 源文件
- `manual_import`：补录结构化财务 CSV

目录位置：

- `build/manual_source/`
- `build/manual_import/`

上传后在页面点击“重建数据库”即可完成重新建库。

## 5. 批量导出与结果导出

```powershell
python run.py export
```

Web 端也支持将当前问答结果或历史记录导出为 `xlsx/json`。

## 6. 生成干净交付包

```powershell
python run.py package
```

生成目录：`build/packages/`

该压缩包会排除以下内容：

- `.venv`
- `.idea`
- `build`
- `result`
- `提交文件`
- `__pycache__`

## 7. 图表 JPG 说明

图表生成改为：

- 优先使用 `Pillow`，具备跨平台能力
- 若当前环境未安装 `Pillow`，Windows 下自动回退到 PowerShell 方案

正式交付与复现环境建议直接安装 `requirements.txt` 中的 `Pillow`。
