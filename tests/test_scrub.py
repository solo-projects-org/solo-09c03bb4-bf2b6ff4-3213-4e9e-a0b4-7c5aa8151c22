from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app import app
from checker.engine import run_check
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.scrub import (
    REQUIRED_MANIFEST_COLUMNS,
    TICKET_COLUMN,
    build_review_package,
    build_synthetic_mapping,
    parse_query_tickets,
    parse_scrub_manifest,
)

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


def sample_manifest_bytes() -> bytes:
    return (SAMPLE / "scrub-manifest.csv").read_bytes()


def sample_uploads() -> dict[str, Path]:
    manifest = parse_manifest(sample_manifest_bytes())
    return {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }


def leaf_index(metadata: pq.FileMetaData, name: str = TICKET_COLUMN) -> int:
    for index in range(metadata.num_columns):
        if metadata.schema.column(index).path == name:
            return index
    raise AssertionError(f"column {name} not found")


def build(tmp_path: Path, tickets: list[int], *, manifest_bytes=None, uploads=None):
    manifest_bytes = (
        sample_manifest_bytes() if manifest_bytes is None else manifest_bytes
    )
    uploads = sample_uploads() if uploads is None else uploads
    return build_review_package(manifest_bytes, uploads, tickets)


def extract(package, tmp_path: Path) -> Path:
    archive = zipfile.ZipFile(io.BytesIO(package.as_zip()))
    target = tmp_path / "package"
    archive.extractall(target)
    return target


# --------------------------------------------------------------------------
# Mapping rules
# --------------------------------------------------------------------------


def test_mapping_queries_get_unique_values_and_intervals_share_one():
    queries = [100, 300]
    observed = {10, 50, 100, 200, 250, 300, 900}
    mapping = build_synthetic_mapping(queries, observed)

    query_synthetics = {mapping[100], mapping[300]}
    assert len(query_synthetics) == 2
    # Same open interval -> same synthetic value.
    assert mapping[10] == mapping[50]
    assert mapping[200] == mapping[250]
    # Distinct interval/query groups never collide; equal values stay equal.
    distinct_groups = [
        mapping[10], mapping[100], mapping[200], mapping[300], mapping[900]
    ]
    assert len(set(distinct_groups)) == len(distinct_groups)
    assert len(set(mapping.values())) == 5
    # Strictly order preserving and always negative.
    ordered = [mapping[v] for v in sorted(observed)]
    assert ordered == sorted(ordered)
    assert all(v < 0 for v in mapping.values())


def test_mapping_absent_query_still_gets_unique_synthetic():
    mapping = build_synthetic_mapping([42, 1000], {1, 2, 3})
    assert 42 in mapping and 1000 in mapping
    assert mapping[42] != mapping[1000]
    assert all(v < 0 for v in mapping.values())
    # The two queries plus the single non-empty interval = three slots.
    assert sorted(set(mapping.values())) == [-3, -2, -1]


def test_mapping_adjacent_queries_do_not_need_empty_interval_slot():
    mapping = build_synthetic_mapping([10, 11], {10, 11})
    assert mapping[10] != mapping[11]
    assert sorted(mapping.values()) == [-2, -1]


def test_mapping_order_preservation_with_equal_values_across_files():
    # 100 and 101 share the open interval between queries 50 and 200.
    observed = {5, 100, 101, 500}
    mapping = build_synthetic_mapping([50, 200], observed)
    ordered = [mapping[v] for v in sorted(observed)]
    assert ordered == sorted(ordered)
    assert mapping[100] == mapping[101]
    # The four observed groups collapse to three interval slots plus the two
    # query points.
    assert all(v < 0 for v in mapping.values())
    assert mapping[5] < mapping[100] < mapping[500]


def test_parse_query_tickets_validation():
    assert parse_query_tickets("1, 2 3；4、5") == [1, 2, 3, 4, 5]
    assert parse_query_tickets("7, 7, 8") == [7, 8]
    for bad in ("", "   ", "abc", "1, -2", "1.5", "+3x"):
        with pytest.raises(CheckError):
            parse_query_tickets(bad)
    with pytest.raises(CheckError):
        parse_query_tickets(str(2**63))
    assert parse_query_tickets(str(2**63 - 1)) == [2**63 - 1]


# --------------------------------------------------------------------------
# Strict manifest handling
# --------------------------------------------------------------------------


def test_json_manifest_is_rejected_for_review_flow():
    payload = (
        '[{"original_path": "s3://b/a.parquet", "uploaded_filename": "a.parquet",'
        ' "file_type": "data", "data_sequence_number": 1}]'
    )
    with pytest.raises(CheckError, match="CSV"):
        parse_scrub_manifest(payload.encode())


def test_manifest_must_have_exactly_four_named_columns():
    extra = (
        ",".join(REQUIRED_MANIFEST_COLUMNS) + ",extra\n"
        "s3://b/a.parquet,a.parquet,data,1,x\n"
    )
    with pytest.raises(CheckError, match="恰好包含四列"):
        parse_scrub_manifest(extra.encode())

    reordered = (
        "uploaded_filename,original_path,file_type,data_sequence_number\n"
        "a.parquet,s3://b/a.parquet,data,1\n"
    )
    with pytest.raises(CheckError, match="恰好包含四列"):
        parse_scrub_manifest(reordered.encode())


def test_equality_delete_entry_is_rejected(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1, 2], type=pa.int64())}), data
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,1\n"
        "s3://b/eq.parquet,eq.parquet,equality_delete,2\n"
    )
    with pytest.raises(CheckError, match="等值删除"):
        build_review_package(
            manifest.encode(), {"data.parquet": data}, [1]
        )


def test_missing_replica_aborts_and_names_file(tmp_path: Path):
    uploads = sample_uploads()
    del uploads["delete-pos-0002.parquet"]
    with pytest.raises(CheckError) as excinfo:
        build(tmp_path, [4242], uploads=uploads)
    assert "delete-pos-0002.parquet" in excinfo.value.files
    assert "缺失" in excinfo.value.message


def test_corrupt_parquet_aborts_and_names_file(tmp_path: Path):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet at all")
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/bad.parquet,bad.parquet,data,1\n"
    )
    with pytest.raises(CheckError) as excinfo:
        build_review_package(
            manifest.encode(), {"bad.parquet": bad}, [1]
        )
    assert "bad.parquet" in excinfo.value.files


# --------------------------------------------------------------------------
# Data-file validation
# --------------------------------------------------------------------------


def _one_file_manifest(name: str = "data.parquet") -> bytes:
    return (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"s3://bucket/data/{name},{name},data,3\n"
    ).encode()


def test_ticket_column_must_be_int64(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(pa.table({TICKET_COLUMN: pa.array([1, 2], type=pa.int32())}), data)
    with pytest.raises(CheckError, match="int64"):
        build_review_package(_one_file_manifest(), {"data.parquet": data}, [1])


def test_missing_ticket_column_is_rejected(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(pa.table({"other": pa.array([1], type=pa.int64())}), data)
    with pytest.raises(CheckError, match="ticket_no"):
        build_review_package(_one_file_manifest(), {"data.parquet": data}, [1])


def test_negative_source_ticket_is_rejected(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1, -9], type=pa.int64())}), data
    )
    with pytest.raises(CheckError, match="负数票号"):
        build_review_package(_one_file_manifest(), {"data.parquet": data}, [1])


def test_mixed_row_group_statistics_are_rejected(tmp_path: Path):
    # PyArrow normalizes statistics at writer level, so the mixed case is
    # exercised against the validator with lightweight stand-in metadata.
    schema = pa.schema([(TICKET_COLUMN, pa.int64())])
    data = tmp_path / "data.parquet"
    with pq.ParquetWriter(data, schema, write_statistics=False) as writer:
        writer.write_table(
            pa.table({TICKET_COLUMN: pa.array([1, 2], type=pa.int64())})
        )
        writer.write_table(
            pa.table({TICKET_COLUMN: pa.array([3, 4], type=pa.int64())})
        )

    class _Stats:
        has_min_max = True
        null_count = 0
        min = 1
        max = 2

    class _Column:
        def __init__(self, has_stats):
            self.statistics = _Stats() if has_stats else None

    class _RowGroup:
        def __init__(self, rg_index):
            self._rg_index = rg_index
            self.num_rows = 2

        def column(self, leaf_index):
            # Group 0 carries stats, group 1 does not: the rejected mix.
            return _Column(self._rg_index == 0)

    class _Leaf:
        path = TICKET_COLUMN

    class _Schema:
        def column(self, index):
            return _Leaf()

    class _Metadata:
        num_row_groups = 2
        num_columns = 1
        schema = _Schema()

        def row_group(self, rg_index):
            return _RowGroup(rg_index)

    class _Parquet:
        schema_arrow = schema
        metadata = _Metadata()

    original = pq.ParquetFile

    def fake_parquet(path):
        if str(path) == str(data):
            return _Parquet()
        return original(str(path))

    pq.ParquetFile = fake_parquet
    try:
        with pytest.raises(CheckError, match="统计不一致"):
            build_review_package(
                _one_file_manifest(), {"data.parquet": data}, [1]
            )
    finally:
        pq.ParquetFile = original


def test_all_null_row_group_keeps_no_statistics_and_boundaries(tmp_path: Path):
    data = tmp_path / "data.parquet"
    schema = pa.schema(
        [(TICKET_COLUMN, pa.int64()), ("secret", pa.string())]
    )
    with pq.ParquetWriter(data, schema) as writer:
        writer.write_table(
            pa.table(
                {
                    TICKET_COLUMN: pa.array([1, None, 3], type=pa.int64()),
                    "secret": ["a", "b", "c"],
                },
                schema=schema,
            )
        )
        writer.write_table(
            pa.table(
                {
                    TICKET_COLUMN: pa.array([None, None], type=pa.int64()),
                    "secret": ["d", "e"],
                },
                schema=schema,
            )
        )
    package = build_review_package(
        _one_file_manifest(), {"data.parquet": data}, [2]
    )
    out = tmp_path / "out.parquet"
    out.write_bytes(package.replica_bytes["data.parquet"])
    result = pq.ParquetFile(out)
    assert result.metadata.num_row_groups == 2
    assert result.schema_arrow.names == [TICKET_COLUMN]
    assert result.metadata.row_group(0).num_rows == 3
    assert result.metadata.row_group(1).num_rows == 2
    null_group_stats = result.metadata.row_group(1).column(0).statistics
    assert null_group_stats is None or not null_group_stats.has_min_max
    values = result.read().column(TICKET_COLUMN).to_pylist()
    assert [v is None for v in values] == [False, True, False, True, True]
    assert all(v < 0 for v in values if v is not None)


def test_no_statistics_file_emits_no_statistics(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {TICKET_COLUMN: pa.array([1, 2, 3], type=pa.int64())}
        ),
        data,
        write_statistics=False,
    )
    package = build_review_package(
        _one_file_manifest(), {"data.parquet": data}, [2]
    )
    out = tmp_path / "out.parquet"
    out.write_bytes(package.replica_bytes["data.parquet"])
    result = pq.ParquetFile(out)
    for index in range(result.metadata.num_row_groups):
        stats = result.metadata.row_group(index).column(0).statistics
        assert stats is None or not stats.has_min_max


def test_source_custom_metadata_is_stripped(tmp_path: Path):
    data = tmp_path / "data.parquet"
    schema = pa.schema(
        [(TICKET_COLUMN, pa.int64())],
        metadata={b"x-internal": b"do-not-leak", b"x-owner": b"finance"},
    )
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1], type=pa.int64())}, schema=schema),
        data,
    )
    source = pq.ParquetFile(data)
    assert source.metadata.metadata is not None
    assert source.schema_arrow.metadata is not None
    package = build_review_package(
        _one_file_manifest(), {"data.parquet": data}, [1]
    )
    out = tmp_path / "out.parquet"
    out.write_bytes(package.replica_bytes["data.parquet"])
    result = pq.ParquetFile(out)
    assert result.metadata.metadata is None
    assert result.schema_arrow.metadata is None


def test_extra_columns_are_dropped_but_rows_and_nulls_kept(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                TICKET_COLUMN: pa.array([5, None, 8], type=pa.int64()),
                "passenger_name": ["Alice", "Bob", "Cara"],
                "fare_cents": pa.array([100, 200, 300], type=pa.int64()),
            }
        ),
        data,
        row_group_size=2,
    )
    package = build_review_package(
        _one_file_manifest(), {"data.parquet": data}, [6]
    )
    out = tmp_path / "out.parquet"
    out.write_bytes(package.replica_bytes["data.parquet"])
    result = pq.ParquetFile(out)
    assert result.schema_arrow.names == [TICKET_COLUMN]
    assert result.metadata.num_row_groups == 2
    assert result.metadata.row_group(0).num_rows == 2
    assert result.metadata.row_group(1).num_rows == 1
    assert result.read().column(TICKET_COLUMN).to_pylist()[1] is None


def test_delete_replica_keeps_only_file_path_and_pos(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1, 2], type=pa.int64())}), data
    )
    delete = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": ["s3://bucket/data/data.parquet"],
                "pos": pa.array([0], type=pa.int64()),
                "row": pa.array([0], type=pa.int64()),
            }
        ),
        delete,
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/data/data.parquet,data.parquet,data,1\n"
        "s3://bucket/delete/delete.parquet,delete.parquet,position_delete,2\n"
    ).encode()
    package = build_review_package(
        manifest,
        {"data.parquet": data, "delete.parquet": delete},
        [1],
    )
    out = tmp_path / "delete-out.parquet"
    out.write_bytes(package.replica_bytes["delete.parquet"])
    result = pq.ParquetFile(out)
    assert result.schema_arrow.names == ["file_path", "pos"]
    assert result.read().to_pydict() == {
        "file_path": ["s3://bucket/data/data.parquet"],
        "pos": [0],
    }
    assert result.metadata.metadata is None


def test_delete_file_missing_pos_column_aborts(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1], type=pa.int64())}), data
    )
    delete = tmp_path / "delete.parquet"
    pq.write_table(pa.table({"file_path": ["s3://bucket/data/data.parquet"]}), delete)
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/data/data.parquet,data.parquet,data,1\n"
        "s3://bucket/delete/delete.parquet,delete.parquet,position_delete,2\n"
    ).encode()
    with pytest.raises(CheckError, match="pos"):
        build_review_package(
            manifest,
            {"data.parquet": data, "delete.parquet": delete},
            [1],
        )


# --------------------------------------------------------------------------
# Package contents
# --------------------------------------------------------------------------


def test_package_zip_contents_and_no_leaks(tmp_path: Path):
    package = build(tmp_path, [4242, 1003, 4218, 8888, 9999])
    archive = zipfile.ZipFile(io.BytesIO(package.as_zip()))
    names = set(archive.namelist())
    assert names == {
        "manifest.csv",
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
        "query-tickets.txt",
    }
    # Manifest is the untouched original bytes.
    assert archive.read("manifest.csv") == sample_manifest_bytes()
    query_lines = archive.read("query-tickets.txt").decode().split()
    assert query_lines == [
        str(item.synthetic) for item in package.query_tickets
    ]
    assert all(line.startswith("-") for line in query_lines)

    # No original query ticket number appears anywhere in the archive.
    originals = {"4242", "1003", "4218", "8888", "9999"}
    for name in names:
        blob = archive.read(name)
        if name.endswith(".parquet"):
            # Raw scan is a cheap extra guard; semantic checks read the file.
            for token in originals:
                assert token.encode() not in blob
    # No mapping table is shipped.
    assert not any("mapping" in name.lower() for name in names)


def test_same_observed_value_maps_consistently_across_files(tmp_path: Path):
    left = tmp_path / "left.parquet"
    right = tmp_path / "right.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([10, 20], type=pa.int64())}), left
    )
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([20, 30], type=pa.int64())}), right
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/left.parquet,left.parquet,data,1\n"
        "s3://b/right.parquet,right.parquet,data,1\n"
    ).encode()
    package = build_review_package(
        manifest,
        {"left.parquet": left, "right.parquet": right},
        [25],
    )
    left_out = pq.read_table(
        pa.BufferReader(package.replica_bytes["left.parquet"])
    ).column(TICKET_COLUMN).to_pylist()
    right_out = pq.read_table(
        pa.BufferReader(package.replica_bytes["right.parquet"])
    ).column(TICKET_COLUMN).to_pylist()
    # Value 20 occurs in both files and must share one synthetic number.
    assert left_out[1] == right_out[0]
    assert left_out != [10, 20] and right_out != [20, 30]
    assert all(v < 0 for v in left_out + right_out)


# --------------------------------------------------------------------------
# End-to-end equivalence against the original checker
# --------------------------------------------------------------------------


def test_rechecked_synthetic_package_matches_original_verdicts(tmp_path: Path):
    queries = [4242, 1003, 4218, 8888, 9999]
    package = build(tmp_path, queries)
    package_dir = extract(package, tmp_path)

    original_manifest = parse_manifest(sample_manifest_bytes())
    synthetic_manifest = parse_manifest(
        (package_dir / "manifest.csv").read_bytes()
    )
    original_uploads = sample_uploads()
    synthetic_uploads = {
        entry.uploaded_filename: package_dir / entry.uploaded_filename
        for entry in synthetic_manifest.required_entries
    }

    original = run_check(original_manifest, original_uploads, queries)
    synthetic = run_check(
        synthetic_manifest,
        synthetic_uploads,
        package.synthetic_query_numbers,
    )

    def signature(result):
        rows = []
        for ticket in result.tickets:
            for occurrence in ticket.occurrences:
                rows.append(
                    (
                        occurrence.file_entry.uploaded_filename,
                        occurrence.row_index,
                        occurrence.row_group_index,
                        tuple(
                            sorted(
                                (
                                    match.entry.uploaded_filename,
                                    match.source_row,
                                )
                                for match in occurrence.matched_deletes
                            )
                        ),
                        tuple(
                            sorted(
                                (
                                    ignored.record.entry.uploaded_filename,
                                    ignored.record.source_row,
                                )
                                for ignored in occurrence.ignored_deletes
                            )
                        ),
                    )
                )
        return sorted(rows)

    def skip_signature(result):
        return sorted(
            (
                scan.file_entry.uploaded_filename,
                tuple(
                    rg.row_group_index
                    for rg in scan.row_groups
                    if rg.skipped
                ),
                tuple(
                    (rg.row_group_index, tuple(rg.matched_rows))
                    for rg in scan.row_groups
                ),
            )
            for ticket in result.tickets
            for scan in ticket.file_scans
        )

    assert signature(original) == signature(synthetic)
    assert skip_signature(original) == skip_signature(synthetic)

    # Row group boundaries and statistics presence are preserved per file.
    for entry in original_manifest.data_files:
        before = pq.ParquetFile(
            original_uploads[entry.uploaded_filename]
        )
        after = pq.ParquetFile(
            synthetic_uploads[entry.uploaded_filename]
        )
        assert after.metadata.num_row_groups == before.metadata.num_row_groups
        assert [
            after.metadata.row_group(i).num_rows
            for i in range(after.metadata.num_row_groups)
        ] == [
            before.metadata.row_group(i).num_rows
            for i in range(before.metadata.num_row_groups)
        ]
        for index in range(before.metadata.num_row_groups):
            old_stats = before.metadata.row_group(index).column(
                leaf_index(before.metadata)
            ).statistics
            new_stats = after.metadata.row_group(index).column(0).statistics
            old_present = bool(
                old_stats is not None and old_stats.has_min_max
            )
            new_present = bool(
                new_stats is not None and new_stats.has_min_max
            )
            assert old_present == new_present


# --------------------------------------------------------------------------
# Flask routes
# --------------------------------------------------------------------------


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as test_client:
        yield test_client


def test_review_page_renders(client):
    response = client.get("/review")
    assert response.status_code == 200
    assert "撤销票复查包" in response.get_data(as_text=True)


def test_review_sample_route_downloads_zip(client):
    response = client.get("/review/sample")
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    assert "query-tickets.txt" in archive.namelist()
    assert "manifest.csv" in archive.namelist()
    assert all(name.endswith(".parquet") or name.endswith((".csv", ".txt"))
               for name in archive.namelist())


def test_review_upload_route_round_trip(client):
    files = []
    for name in sample_uploads():
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    response = client.post(
        "/review/package",
        data={
            "manifest": (
                io.BytesIO(sample_manifest_bytes()),
                "scrub-manifest.csv",
            ),
            "files": files,
            "tickets": "4242, 1003",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    query_numbers = [
        int(line)
        for line in archive.read("query-tickets.txt").decode().split()
    ]
    assert len(query_numbers) == 2
    assert all(value < 0 for value in query_numbers)


def test_review_route_rejects_equality_delete_manifest(client, tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1], type=pa.int64())}), data
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,1\n"
        "s3://b/eq.parquet,eq.parquet,equality_delete,2\n"
    )
    response = client.post(
        "/review/package",
        data={
            "manifest": (io.BytesIO(manifest.encode()), "manifest.csv"),
            "files": [
                (io.BytesIO(data.read_bytes()), "data.parquet"),
            ],
            "tickets": "1",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "复查包未生成" in html
    assert "等值删除" in html


def test_review_route_rejects_negative_query(client):
    response = client.post(
        "/review/package",
        data={"tickets": "-4242"},
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    # Jinja escapes the quoted token; assert on the stable Chinese phrase.
    assert "不是非负整数" in html
    assert "复查包未生成" in html


def test_review_route_requires_manifest(client):
    response = client.post(
        "/review/package",
        data={"tickets": "1"},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "四列标准" in response.get_data(as_text=True)


def test_path_like_uploaded_filename_is_rejected(tmp_path: Path):
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1], type=pa.int64())}), data
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,subdir/data.parquet,data,1\n"
    ).encode()
    with pytest.raises(CheckError, match="纯文件名"):
        build_review_package(
            manifest, {"subdir/data.parquet": data}, [1]
        )


def test_reserved_replica_name_collision_is_rejected(tmp_path: Path):
    data = tmp_path / "manifest.csv"
    pq.write_table(
        pa.table({TICKET_COLUMN: pa.array([1], type=pa.int64())}), data
    )
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,manifest.csv,data,1\n"
    ).encode()
    package = build_review_package(
        manifest, {"manifest.csv": data}, [1]
    )
    with pytest.raises(CheckError, match="保留文件冲突"):
        package.as_zip()


def test_checker_page_reproduces_verdicts_on_extracted_package(
    client, tmp_path: Path
):
    # Build the vendor package, extract it, then feed it back through the
    # original /check page using the synthetic query numbers.
    package = build(tmp_path, [4242, 1003, 4218, 8888, 9999])
    package_dir = extract(package, tmp_path / "roundtrip")

    files = [
        (
            io.BytesIO((package_dir / name).read_bytes()),
            name,
            "application/octet-stream",
        )
        for name in package.replica_bytes
    ]
    synthetic_tickets = ", ".join(
        str(value) for value in package.synthetic_query_numbers
    )
    response = client.post(
        "/check",
        data={
            "manifest": (
                io.BytesIO((package_dir / "manifest.csv").read_bytes()),
                "manifest.csv",
            ),
            "files": files,
            "tickets": synthetic_tickets,
            "ticket_column": TICKET_COLUMN,
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "核对结果" in html
    # The deleted synthetic tickets (-4 for 4242, -7 for 1003, -2 for 8888)
    # must still be flagged deleted with the same delete file.
    assert "delete-pos-0001.parquet" in html
    assert "delete-pos-0002.parquet" in html
    assert "delete-pos-stale-0001.parquet" in html
    # Row-group pruning diagnostics survive the rewrite.
    assert "跳过 4 / 5 个行组" in html
    # The rendered ticket cards only show synthetic (negative) numbers.
    rendered_tickets = re.findall(
        r'<span class="ticket-no">(-?\d+)</span>', html
    )
    assert rendered_tickets
    assert all(int(value) < 0 for value in rendered_tickets)
    assert set(rendered_tickets) == set(
        str(value) for value in package.synthetic_query_numbers
    )
