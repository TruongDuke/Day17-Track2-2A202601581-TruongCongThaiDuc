#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# Ba quyết định của bài A, và lý do của từng cái:
#
#   PARTITION_BY (event_date)
#       Truy vấn dashboard lọc theo hai cột: customer_name và ngày. Chỉ một
#       trong hai được lên đường dẫn, vì mỗi cột partition nhân số thư mục lên.
#       event_date có 14 giá trị -> 14 thư mục, mỗi thư mục ~9.335 hàng: engine
#       đọc tên thư mục là loại được 13/14 dataset mà không mở file nào.
#       customer_name có 650 giá trị -> 650 thư mục ~200 hàng, tức tái tạo lại
#       đúng small-file problem đang phải sửa. Nó được xử lý ở tầng row group
#       bên dưới thay vì ở tầng đường dẫn.
#
#   ORDER BY customer_name, event_time
#       Thống kê min/max của một row group chỉ có ích khi các hàng cùng một
#       khách nằm liền nhau. Nếu thứ tự ngẫu nhiên thì row group nào cũng chứa
#       'ACME' lẫn 'ZYX', min/max phủ toàn bảng chữ cái và không loại được gì.
#       Sắp theo customer_name làm mỗi row group chỉ phủ một dải hẹp tên khách.
#
#   ROW_GROUP_SIZE = 1024
#       Mặc định 122.880 hàng gói trọn cả ngày (9.335 hàng) vào MỘT row group:
#       min/max của nó phủ toàn bộ 650 khách, tức vô dụng. 1024 hàng chia mỗi
#       ngày thành ~10 row group, mỗi group phủ ~65 khách -> đọc đúng một group
#       thay vì cả ngày. Nhỏ hơn nữa thì phần metadata bắt đầu lấn phần dữ liệu.
PARTITION_COL = "event_date"
SORT_COLS = ("customer_name", "event_time")
ROW_GROUP_SIZE = 1024


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_before = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC}/*.parquet')
            order by {', '.join(SORT_COLS)}
        ) to '{DST}' (
            format          parquet,
            partition_by    ({PARTITION_COL}),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]
    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    n_dst = len(list(DST.rglob("*.parquet")))
    print(f"  đích  : {DST}  ({n_dst:,} file, "
          f"{len(list(DST.glob(PARTITION_COL + '=*'))):,} partition)")
    print(f"  số hàng giữ nguyên: {n_before:,}")
    print(f"\n  xong. Đo lại bằng: make explain\n")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
