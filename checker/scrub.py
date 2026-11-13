"""Build a vendor-safe review package from an Iceberg v2 snapshot.

The duty officer hands the query-engine vendor a *synthetic* copy of the
snapshot that still reproduces every cancel-ticket verdict, without exposing
any real ticket number or any other ticket-bearing column.

Synthetic ticket numbers are always negative (real data is non-negative):

* Each queried ticket gets a unique negative number, even when the ticket is
  absent from the data files.
* Every other non-null ticket is replaced according to the open interval it
  falls into relative to the sorted queried tickets: below the first query,
  between two consecutive queries, or above the last.  All values in the same
  interval share one negative number.
* Order is preserved: ``x < y`` implies ``synth(x) < synth(y)`` and equal
  values keep the same replacement, so equality matches, min/max pruning and
  row-group skip decisions are identical after re-checking.

Guarantees of the produced Parquet replicas:

* Data replicas keep only the ``ticket_no`` column (int64).  Null positions,
  row order, row count and row-group boundaries are untouched.
* Position-delete replicas keep only ``file_path`` and ``pos``; the manifest
  is copied byte-for-byte, so delete verdicts are unaffected.
* No source custom Parquet key/value metadata (``ARROW:schema`` included) is
  carried over.
* Per data file, every non-all-null row group either keeps full ticket
  min/max statistics or none of them do; an all-null row group naturally has
  no statistics on both sides.

Equality-delete manifest entries are refused, as are unsupported inputs,
missing replicas, non-int64 / negative ticket values and mixed statistics.
Any failure raises :class:`CheckError`; the caller must not ship a package.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import CheckError
from .manifest import Manifest, ManifestEntry, parse_manifest

TICKET_COLUMN = "ticket_no"
DELETE_PATH_COLUMN = "file_path"
DELETE_POS_COLUMN = "pos"

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

# Exactly these four columns, in this order, for the standard Iceberg v2 CSV.
REQUIRED_MANIFEST_COLUMNS = (
    "original_path",
    "uploaded_filename",
    "file_type",
    "data_sequence_number",
)


@dataclass(frozen=True)
class SyntheticTicket:
    original: int
    synthetic: int


@dataclass(frozen=True)
class DataFileStats:
    entry: ManifestEntry
    num_row_groups: int
    # True when every non-all-null row group exposes min/max statistics,
    # False when none of them do.
    statistics_enabled: bool
    all_null_row_groups: tuple[int, ...]


@dataclass(frozen=True)
class ReviewPackage:
    manifest_bytes: bytes
    # uploaded_filename -> rewritten parquet bytes
    replica_bytes: Mapping[str, bytes]
    query_tickets: tuple[SyntheticTicket, ...]

    @property
    def synthetic_query_numbers(self) -> list[int]:
        return [item.synthetic for item in self.query_tickets]

    def as_zip(self) -> bytes:
        """Serialize the package.

        The archive contains the unchanged manifest, one rewritten replica per
        required manifest entry, and ``query-tickets.txt`` listing only the
        synthetic query numbers.  No original ticket, mapping table or source
        metadata is included.
        """
        buffer = io.BytesIO()
        reserved = {"manifest.csv", "query-tickets.txt"}
        collision = reserved.intersection(self.replica_bytes)
        if collision:
            raise CheckError(
                "副本文件名与复查包保留文件冲突：" + "、".join(sorted(collision))
            )
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.csv", self.manifest_bytes)
            for name, data in self.replica_bytes.items():
                archive.writestr(name, data)
            lines = [
                str(item.synthetic) for item in self.query_tickets
            ]
            archive.writestr(
                "query-tickets.txt",
                ("\n".join(lines) + "\n") if lines else "",
            )
        return buffer.getvalue()


def parse_query_tickets(text: str) -> list[int]:
    """Parse queried ticket numbers: non-negative int64, deduplicated."""
    tokens = [
        token.strip()
        for token in re.split(r"[,\s，、;；]+", str(text or ""))
        if token.strip()
    ]
    if not tokens:
        raise CheckError("请输入至少一个待查票号。")
    tickets: list[int] = []
    seen: set[int] = set()
    for token in tokens:
        if not (token.lstrip("+").isdigit()):
            raise CheckError(
                f"待查票号 “{token}” 不是非负整数；合成值才使用负数。"
            )
        value = int(token)
        if value > INT64_MAX:
            raise CheckError(
                f"待查票号 “{token}” 超过 int64 上限 {INT64_MAX}。"
            )
        if value not in seen:
            seen.add(value)
            tickets.append(value)
    return tickets


def parse_scrub_manifest(data: bytes) -> Manifest:
    """Parse the strict four-column CSV manifest used by the review flow."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CheckError(f"清单不是有效的 UTF-8 CSV 文本：{exc}") from None
    stripped = text.lstrip("﻿ \t\r\n")
    if not stripped:
        raise CheckError("清单为空。")
    if stripped.lstrip().startswith(("[", "{")):
        raise CheckError(
            "复查包流程仅支持四列标准 Iceberg v2 CSV 清单，不支持 JSON。"
        )
    try:
        reader = csv.reader(io.StringIO(text))
        header = next(reader, None)
    except csv.Error as exc:
        raise CheckError(f"CSV 清单解析失败：{exc}") from None
    if not header:
        raise CheckError("清单为空或缺少表头。")
    columns = [column.strip() for column in header]
    if columns != list(REQUIRED_MANIFEST_COLUMNS):
        raise CheckError(
            "清单必须恰好包含四列且列名为 "
            f"{', '.join(REQUIRED_MANIFEST_COLUMNS)}；实际列为："
            f"{', '.join(columns) if columns else '（空）'}。"
        )
    # The generic parser re-validates types, aliases and uniqueness.
    return parse_manifest(text)


def _require_int64_ticket_column(parquet: pq.ParquetFile, label: str) -> None:
    try:
        field = parquet.schema_arrow.field(TICKET_COLUMN)
    except KeyError:
        raise CheckError(
            f"数据文件 “{label}” 缺少 int64 票号列 “{TICKET_COLUMN}”。",
            files=[label],
        ) from None
    if field.type != pa.int64():
        raise CheckError(
            f"数据文件 “{label}” 的票号列类型为 {field.type}，"
            "源票号列必须是 int64。",
            files=[label],
        )


def _ticket_leaf_index(metadata: pq.FileMetaData, label: str) -> int:
    for index in range(metadata.num_columns):
        if metadata.schema.column(index).path == TICKET_COLUMN:
            return index
    raise CheckError(
        f"数据文件 “{label}” 的 Parquet 元数据中找不到列 “{TICKET_COLUMN}”。",
        files=[label],
    )


def _inspect_data_file(
    entry: ManifestEntry, path: Path
) -> tuple[pq.ParquetFile, DataFileStats]:
    label = entry.uploaded_filename
    try:
        parquet = pq.ParquetFile(str(path))
    except Exception as exc:
        raise CheckError(
            f"无法读取数据文件 “{label}”（原始路径 {entry.original_path}）："
            f"{exc}",
            files=[label],
        ) from None
    _require_int64_ticket_column(parquet, label)

    metadata = parquet.metadata
    leaf_index = _ticket_leaf_index(metadata, label)
    presence: list[bool] = []
    all_null_groups: list[int] = []
    try:
        for rg_index in range(metadata.num_row_groups):
            column_meta = metadata.row_group(rg_index).column(leaf_index)
            stats = column_meta.statistics
            has_min_max = bool(
                stats is not None and getattr(stats, "has_min_max", False)
            )
            null_count = int(getattr(stats, "null_count", 0) or 0)
            is_all_null = null_count == int(
                metadata.row_group(rg_index).num_rows
            )
            if is_all_null:
                all_null_groups.append(rg_index)
                if has_min_max:
                    raise CheckError(
                        f"数据文件 “{label}” 的行组 {rg_index} 全为空值却带有"
                        " min/max 统计，输入不受支持。",
                        files=[label],
                    )
            else:
                presence.append(has_min_max)
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"无法读取数据文件 “{label}” 的行组元数据：{exc}",
            files=[label],
        ) from None

    if presence and not all(presence) and any(presence):
        raise CheckError(
            f"数据文件 “{label}” 的非全空行组票号统计不一致：部分行组有 "
            "min/max、部分缺失。每个数据文件的非全空行组统计须全有或全无。",
            files=[label],
        )
    stats_enabled = bool(presence) and all(presence)
    return parquet, DataFileStats(
        entry=entry,
        num_row_groups=metadata.num_row_groups,
        statistics_enabled=stats_enabled,
        all_null_row_groups=tuple(all_null_groups),
    )


def _collect_distinct_values(
    handles: Mapping[str, pq.ParquetFile]
) -> set[int]:
    distinct: set[int] = set()
    for entry_path, parquet in handles.items():
        label = entry_path
        try:
            table = parquet.read(columns=[TICKET_COLUMN])
        except Exception as exc:
            raise CheckError(
                f"读取数据文件 “{label}” 的票号列失败：{exc}",
                files=[label],
            ) from None
        for value in table.column(TICKET_COLUMN).to_pylist():
            if value is None:
                continue
            if value < 0:
                raise CheckError(
                    f"数据文件 “{label}” 含有负数票号 {value}；源票号必须为"
                    "非负 int64，负数是合成值的保留区间。",
                    files=[label],
                )
            distinct.add(int(value))
    return distinct


def build_synthetic_mapping(
    query_tickets: list[int], observed_values: set[int]
) -> dict[int, int]:
    """Assign strictly increasing negative synthetic numbers.

    The value line is split into ordered slots: one slot per queried ticket
    and one open-interval slot between consecutive queries (plus the two
    outside ends).  Only slots containing an observed value or a query are
    materialized.  Queried tickets always receive their own unique value,
    including tickets that never occur in the data; all observed values in
    the same interval share one value.
    """
    queries = sorted(set(query_tickets))
    observed = sorted(observed_values)

    # Bounds of each slot in order: query points alternate with intervals.
    slots: list[tuple[int | None, int | None]] = []
    for index, query in enumerate(queries):
        lower = queries[index - 1] if index > 0 else None
        upper = queries[index + 1] if index + 1 < len(queries) else None
        slots.append((lower, query))  # open interval below this query
        slots.append((query, query))  # the query point itself
    slots.append((queries[-1], None))  # open interval above the last query

    used: list[tuple[int | None, int | None]] = []
    for lower, upper in slots:
        if lower is not None and upper is not None and lower == upper:
            used.append((lower, upper))  # query point slot, always materialized
            continue
        in_slot = any(
            (lower is None or value > lower)
            and (upper is None or value < upper)
            for value in observed
        )
        if in_slot:
            used.append((lower, upper))

    total = len(used)
    mapping: dict[int, int] = {}
    for position, (lower, upper) in enumerate(used):
        synthetic = -total + position
        if lower is not None and upper is not None and lower == upper:
            mapping[lower] = synthetic
        else:
            for value in observed:
                if (lower is None or value > lower) and (
                    upper is None or value < upper
                ):
                    mapping[value] = synthetic
    return mapping


def _rewrite_data_file(
    entry: ManifestEntry,
    parquet: pq.ParquetFile,
    stats: DataFileStats,
    mapping: Mapping[int, int],
) -> bytes:
    label = entry.uploaded_filename
    out_schema = pa.schema([(TICKET_COLUMN, pa.int64())])
    buffer = io.BytesIO()
    try:
        with pq.ParquetWriter(
            buffer,
            out_schema,
            store_schema=False,
            write_statistics=stats.statistics_enabled,
            compression="snappy",
        ) as writer:
            for rg_index in range(stats.num_row_groups):
                table = parquet.read_row_group(
                    rg_index, columns=[TICKET_COLUMN]
                )
                original = table.column(TICKET_COLUMN).to_pylist()
                try:
                    replaced = [
                        None
                        if value is None
                        else mapping[int(value)]
                        for value in original
                    ]
                except KeyError as exc:
                    raise CheckError(
                        f"数据文件 “{label}” 的票号 {exc.args[0]} 没有对应的"
                        "合成值，拒绝发包。",
                        files=[label],
                    ) from None
                row_group = pa.table(
                    {TICKET_COLUMN: pa.array(replaced, type=pa.int64())}
                )
                writer.write_table(row_group)
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"重写数据文件 “{label}” 失败：{exc}", files=[label]
        ) from None
    return buffer.getvalue()


def _rewrite_delete_file(entry: ManifestEntry, path: Path) -> bytes:
    label = entry.uploaded_filename
    try:
        parquet = pq.ParquetFile(str(path))
        schema = parquet.schema_arrow
        try:
            path_field = schema.field(DELETE_PATH_COLUMN)
        except KeyError:
            raise CheckError(
                f"位置删除文件 “{label}” 缺少列 “{DELETE_PATH_COLUMN}”。",
                files=[label],
            ) from None
        if not (
            pa.types.is_string(path_field.type)
            or pa.types.is_large_string(path_field.type)
        ):
            raise CheckError(
                f"位置删除文件 “{label}” 的 “{DELETE_PATH_COLUMN}” 类型为 "
                f"{path_field.type}，不是字符串类型。",
                files=[label],
            )
        try:
            pos_field = schema.field(DELETE_POS_COLUMN)
        except KeyError:
            raise CheckError(
                f"位置删除文件 “{label}” 缺少列 “{DELETE_POS_COLUMN}”。",
                files=[label],
            ) from None
        if not pa.types.is_integer(pos_field.type):
            raise CheckError(
                f"位置删除文件 “{label}” 的 “{DELETE_POS_COLUMN}” 类型为 "
                f"{pos_field.type}，不是整数类型。",
                files=[label],
            )
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"无法读取位置删除文件 “{label}”（原始路径 "
            f"{entry.original_path}）：{exc}",
            files=[label],
        ) from None

    buffer = io.BytesIO()
    out_schema = pa.schema(
        [
            pa.field(DELETE_PATH_COLUMN, path_field.type),
            pa.field(DELETE_POS_COLUMN, pos_field.type),
        ]
    )
    try:
        # Keep only file_path and pos; row order and row-group boundaries
        # are copied group by group, and no source metadata is carried over.
        with pq.ParquetWriter(
            buffer,
            out_schema,
            store_schema=False,
            write_statistics=True,
            compression="snappy",
        ) as writer:
            for rg_index in range(parquet.metadata.num_row_groups):
                writer.write_table(
                    parquet.read_row_group(
                        rg_index,
                        columns=[DELETE_PATH_COLUMN, DELETE_POS_COLUMN],
                    )
                )
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"重写位置删除文件 “{label}” 失败：{exc}", files=[label]
        ) from None
    return buffer.getvalue()


def build_review_package(
    manifest_bytes: bytes,
    uploaded_paths: Mapping[str, Path],
    query_tickets: list[int],
) -> ReviewPackage:
    """Validate everything first, then produce the synthetic package.

    The manifest is never modified; only Parquet replicas are rewritten.
    """
    if not query_tickets:
        raise CheckError("没有待查票号，无法生成复查包。")
    manifest = parse_scrub_manifest(manifest_bytes)

    if manifest.equality_delete_files:
        names = ", ".join(
            entry.uploaded_filename for entry in manifest.equality_delete_files
        )
        raise CheckError(
            "清单含有等值删除（equality_delete）条目，复查包流程拒绝处理："
            f"{names}。请先移除等值删除文件。",
            files=[entry.uploaded_filename for entry in manifest.equality_delete_files],
        )
    if not manifest.data_files:
        raise CheckError("清单中没有 data 数据文件，无法生成复查包。")

    missing = [
        entry.uploaded_filename
        for entry in manifest.required_entries
        if entry.uploaded_filename not in uploaded_paths
        or not Path(uploaded_paths[entry.uploaded_filename]).is_file()
    ]
    if missing:
        raise CheckError(
            "清单中以下文件的上传副本缺失，无法生成复查包。",
            files=missing,
        )

    # Archive entry names must stay plain file names, never paths that could
    # escape the extraction directory.
    unsafe_names = [
        entry.uploaded_filename
        for entry in manifest.entries
        if "/" in entry.uploaded_filename
        or "\\" in entry.uploaded_filename
        or entry.uploaded_filename in ("", ".", "..")
        or entry.uploaded_filename.startswith("~")
    ]
    if unsafe_names:
        raise CheckError(
            "uploaded_filename 只能是不含路径分隔符的纯文件名，"
            "不能包含 /、\\ 或相对路径片段。",
            files=unsafe_names,
        )

    data_handles: dict[str, pq.ParquetFile] = {}
    data_stats: dict[str, DataFileStats] = {}
    for entry in manifest.data_files:
        path = uploaded_paths[entry.uploaded_filename]
        parquet, stats = _inspect_data_file(entry, path)
        data_handles[entry.uploaded_filename] = parquet
        data_stats[entry.uploaded_filename] = stats

    observed = _collect_distinct_values(data_handles)
    mapping = build_synthetic_mapping(query_tickets, observed)

    if any(
        not INT64_MIN <= synthetic <= INT64_MAX
        for synthetic in mapping.values()
    ):  # pragma: no cover - defensive, slots are bounded by input size
        raise CheckError("合成票号超出 int64 范围，拒绝发包。")

    # Every query, present or absent, must have a unique synthetic number.
    synthetic_queries: list[SyntheticTicket] = [
        SyntheticTicket(original=query, synthetic=mapping[query])
        for query in dict.fromkeys(query_tickets)
    ]
    if len({item.synthetic for item in synthetic_queries}) != len(
        synthetic_queries
    ):  # pragma: no cover - defensive
        raise CheckError("合成待查票号出现冲突，拒绝发包。")

    replica_bytes: dict[str, bytes] = {}
    for entry in manifest.data_files:
        replica_bytes[entry.uploaded_filename] = _rewrite_data_file(
            entry,
            data_handles[entry.uploaded_filename],
            data_stats[entry.uploaded_filename],
            mapping,
        )
    for entry in manifest.position_delete_files:
        replica_bytes[entry.uploaded_filename] = _rewrite_delete_file(
            entry, uploaded_paths[entry.uploaded_filename]
        )

    return ReviewPackage(
        manifest_bytes=manifest_bytes,
        replica_bytes=replica_bytes,
        query_tickets=tuple(synthetic_queries),
    )
