# 渡轮结算 · Iceberg v2 票号删除核对

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页接收有效文件清单和对应 Parquet 副本，输入整数
票号，逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。

此外提供 **撤销票复查包** 入口：在不交出真实票据明细的前提下，生成一份可交给
查询引擎供应商复查的合成样本（ZIP）。供应商解压后用同一个核查页查合成票号，
跳过的行组、命中的原始行号与删票依据与替换前完全一致。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 撤销票复查包（合成脱敏）

值班员要把样本交给查询引擎供应商复查撤销票，但不能交出真实票据明细。首页点
**“撤销票复查包”**（路由 `/review`），上传四列标准 Iceberg v2 CSV 清单与
Parquet 副本，输入待查票号，即可下载一个 ZIP 复查包。

### 输入要求

- 清单**仅支持 CSV**，且必须恰好包含四列、列名与顺序固定为
  `original_path,uploaded_filename,file_type,data_sequence_number`；
  JSON、缺列、多列、换序都会被拒绝。
- 票号列固定为 `ticket_no`，源列类型必须是 **int64**；非空源票号与待查票号都
  必须是**非负 int64**。负数是合成值的保留区间，源数据出现负数会被拒绝。
- 清单**含等值删除（equality_delete）条目时拒绝**，不生成复查包。
- 清单中每个 `data` / `position_delete` 条目都必须有对应 Parquet 副本，缺件、
  文件损坏或读写失败都会说明原因且不发包。

### 票号合成规则

合成票号全部为负数，并保持严格的大小顺序（`x < y` ⇒ `合成(x) < 合成(y)`，
相等值保持同一合成号），因此等值匹配、min/max 行组裁剪与跳读决策在复查时
完全复现：

- 每个待查票号换成**互不相同**的负数合成号；即使该票号在数据中不存在，也会
  分配独立合成号并写入 `query-tickets.txt`。
- 其余非空票号只按**相对待查票号的大小区间**替换：小于最小待查票号、落在相邻
  两个待查票号之间、或大于最大待查票号；同一区间内的所有票号用同一个合成值。
- 空值不参与替换，空值位置保持不变。

### 副本与清单处理

- **数据副本只保留 `ticket_no` 一列**：其余列（姓名、金额等真实明细）全部丢弃；
  行数、行序、空值位置与行组边界（每个行组的行数）保持不变。
- 每个数据文件内，非全空行组的票号 min/max 统计**全有或全无**：源文件有统计就
  按原行组重新生成统计，源文件没有就整体不写统计；全空行组两侧都没有统计。
- **删除副本只保留 `file_path` 和 `pos`** 两列，按原行组逐组复制；清单
  **原样打包、不做任何修改**，因此删除匹配（路径 + 行号 + 序列号）结论不变。
- 输出 Parquet 不携带任何源文件自定义键值元数据（含 `ARROW:schema`）。

### ZIP 内容

```text
manifest.csv            与上传清单逐字节相同
data-*.parquet          仅含 ticket_no 的合成数据副本
delete-pos-*.parquet    仅含 file_path、pos 的删除副本
query-tickets.txt       仅合成后的待查票号（每行一个负数）
```

包内**不含**任何原票号、原票号到合成号的映射表，也不含源 Parquet 自定义
元数据。`uploaded_filename` 只能是不含路径分隔符的纯文件名，避免 ZIP 路径
穿越；保留文件名 `manifest.csv`、`query-tickets.txt` 不允许被副本占用。

### 供应商复查方式

解压 ZIP 后，回到删除核对页（`/`），上传其中的清单与全部 Parquet 副本，票号
填入 `query-tickets.txt` 里的合成号。每个合成票号跳过的行组、命中的原始行号
（从 0 起算）以及命中/被忽略的删除文件依据，都与值班员替换前对原票号的核对
结果一致。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

复查包流程使用同一份 Parquet 样例，但清单是
`sample/scrub-manifest.csv`：它去掉了等值删除条目（复查流程拒绝等值删除），
其余条目与 `manifest.csv` 相同。在 `/review` 页点 **“载入本地样例并下载
复查包”**（路由 `/review/sample`）即可直接下载样例复查包。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 68 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析（含复查包路由）
checker/manifest.py        清单解析与校验
checker/engine.py          PyArrow 行组跳读、行号计算、位置删除匹配
checker/scrub.py           票号合成映射、Parquet 脱敏重写、复查包 ZIP
checker/errors.py          CheckError（始终带文件名）
templates/index.html       删除核对网页
templates/scrub.html       复查包生成网页
static/style.css           样式
sample/                    内置样例（清单 + Parquet + 复查专用清单）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；适用于常见规模。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
