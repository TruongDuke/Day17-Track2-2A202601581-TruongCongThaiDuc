# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trương Công Thái Đức  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

Nguyên văn, không cắt bớt — bản rời nộp kèm: [VERIFY_OUTPUT.txt](VERIFY_OUTPUT.txt).

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 16.2s
  run 2/3 … 17.1s
  run 3/3 … 16.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

<details>
<summary>Output <code>make crash-test</code> (bài mở rộng B)</summary>

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**. Cả hai bài mở rộng trong EXTRA.md cũng đã làm
(mục 4 bên dưới): bài A hiện là ba dòng `dashboard` ✓ trong bảng trên, bài B
kiểm bằng `make crash-test`.

> **Thứ tự chạy lại trên repo nộp.** Vì bài mở rộng A có làm, hãy chạy đúng
> trình tự sau — `data/` không được commit nên cần sinh lại:
>
> ```bash
> make setup        # venv + thư viện + 14 ngày dữ liệu seed
> make seed-extra   # sinh data/gold_events/ cho bài mở rộng A
> make compact      # gom lại thành data/gold_events_v2/ (dataset mà dashboard.sql trỏ tới)
> make verify       # 4/4 tiêu chí
> make explain      # bài mở rộng A
> make crash-test   # bài mở rộng B
> ```
>
> Bỏ qua hai bước giữa thì `make verify` dừng giữa chừng với
> `IOException: No files found that match the pattern "data/gold_events*"`.
> Đây là hành vi sẵn có của `tools/verify.py`: nó luôn gọi `dashboard_check()`
> vì `expected/dashboard_baseline.json` được commit kèm repo gốc, trong khi
> `data/` thì không. Tôi không sửa `tools/verify.py` và `tools/explain.py` vì
> RUBRIC cấm.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` = 38.750 hàng thay vì 12.480; mỗi lượt chạy lại tăng thêm, checksum ba lượt khác nhau (`7c461563f4` → `d11657ff21` → `2b76a4f850`). Không có lỗi nào được ném ra. |
| **Nguyên nhân** | Model khai `materialized = 'incremental'` nhưng **không khai `unique_key`**. Không có khoá, dbt không biết hàng nào là "cùng một hàng", nên sinh ra câu `INSERT INTO ... SELECT` thuần. Với câu lệnh đó, chạy lại cùng một partition ngày là **ghi thêm**, không phải ghi đè — tính idempotent nằm ở *câu lệnh ghi*, không ở dữ liệu đầu vào. Mệnh đề `WHERE _ingested_at` theo `run_date` hoàn toàn đúng và không phải nguyên nhân. |
| **Cách khắc phục** | [dbt/models/gold/gold_training_set.sql](dbt/models/gold/gold_training_set.sql) — thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.<br>[dags/ai_training_pipeline.py](dags/ai_training_pipeline.py) — `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp) · sau: 12.480 hàng, 0 ticket lặp · checksum 3 lượt: `8dd7c98653` × 3 |

**Vì sao `merge` chứ không phải `delete+insert`.** Grain của bảng là **entity**
(1 hàng / 1 ticket), khoá tự nhiên là `ticket_id`. Nguồn CDC có 1.310 bản ghi
`op='u'`: một ticket tạo ngày D1 rồi sửa ngày D2 sẽ lọt qua mệnh đề `WHERE`
theo `run_date` **hai lần trong cùng một lượt chạy**, ở hai partition ngày khác
nhau. `delete+insert` theo partition ngày xoá đúng partition đang ghi, nên hai
lần ghi đó không xoá được nhau — ticket vẫn bị nhân đôi. `merge` theo
`ticket_id` thì lần ghi sau thay thế lần ghi trước bất kể nó thuộc partition nào.

**Về hai tham số DAG.** `catchup=True` khiến Airflow tự schedule chạy bù mọi
ngày kể từ `start_date`, `max_active_runs` không đặt cho phép nhiều run ghi
đồng thời vào cùng một bảng. Hai tham số này chỉ **làm tăng tần suất kích hoạt**
lỗi (Clear Task → chạy lại → cộng dồn), chúng **không phải root cause**: sửa DAG
mà không sửa `config()` của model thì `make verify` vẫn đỏ.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` = 8.645 hàng thay vì 9.100 (14 ngày × 650 customer), thiếu 455. Bảng **ỔN ĐỊNH ✓** — chạy lại vẫn ra đúng con số sai đó. Chỉ thiếu ở các ngày đã chốt từ lâu. |
| **P99 độ trễ đo được** | **2,726 ngày** (P50 = 0,128 · P95 = 1,814 · max = 2,945 · 5,05% bản ghi tới muộn hơn 1 ngày) |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2,726 ngày, làm tròn lên đơn vị ngày là 3; con số này cũng phủ luôn `max` = 2,945 ngày nên không bỏ sót bản ghi nào. |
| **Nguyên nhân** | Điều kiện lọc incremental dùng **watermark theo `event_date` của bảng đích**: `where event_date > (select max(event_date) from {{ this }})`. Watermark này đo *thời điểm sự kiện xảy ra*, trong khi cái quyết định bản ghi có mặt trong Silver hay không là *thời điểm nó tới kho* (`_ingested_at`). Một bản ghi xảy ra 08-12 nhưng tới kho 08-15 chỉ xuất hiện trong Silver từ lượt chạy 08-15 trở đi; lúc đó `max(event_date)` trong đích đã là 08-14, nên `08-12 > 08-14` sai — bản ghi bị bỏ qua, và **bị bỏ qua vĩnh viễn** vì watermark chỉ tiến chứ không lùi. Đó là lý do bảng ổn định mà vẫn thiếu hàng. |
| **Cách khắc phục** | [dbt/models/gold/gold_feature_daily.sql](dbt/models/gold/gold_feature_daily.sql) — đổi điều kiện thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, đồng thời thêm `unique_key = ['event_date', 'customer_id']` + `incremental_strategy = 'merge'`. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng · checksum 3 lượt `f8d3f591f0` × 3 |

**Vì sao đổi `>` thành `>=` là chưa đủ.** `>=` chỉ nới window đúng một ngày
(ngày mới nhất), trong khi phân bố đo được cho thấy dữ liệu tới muộn tới 3 ngày:
14.165 bản ghi trễ 1 ngày, 3.842 trễ 2 ngày, 2.593 trễ 3 ngày. Cần một cửa sổ
thực sự lùi về quá khứ, không phải một phép chỉnh biên.

**Ràng buộc đi kèm.** Window rộng hơn nghĩa là cùng một cặp
`(event_date, customer_id)` được tính lại ở nhiều lượt chạy. Nếu model chỉ biết
`insert` thì các lần tính sẽ cộng dồn — tái tạo đúng lỗi của nhiệm vụ 1 trên
một bảng khác. Grain ở đây có **hai cột**, nên `unique_key` phải là một list.

**Vì sao chọn P99 làm căn cứ thay vì `max`.** `max` là một quan sát đơn lẻ:
nó có thể nhảy vọt vì một sự cố cá biệt của nguồn (một lần backfill, một lần
consumer chết ba ngày), và nếu lấy nó làm tham số thì một ngoại lệ trong quá
khứ sẽ bắt pipeline trả giá mãi mãi. P99 là một ngưỡng bền vững hơn: 99% bản
ghi nằm dưới nó. Cái giá của mỗi ngày lùi thêm không trả một lần mà trả ở
**mọi lượt chạy sau này** — window 3 ngày là đọc và ghi lại 3× số cặp mỗi ngày,
window 14 ngày là 14×, tức chi phí tuyến tính theo độ rộng window cho đến hết
vòng đời của bảng. Ở bộ dữ liệu này P99 (2,726) và max (2,945) tình cờ cùng
làm tròn lên 3 ngày, nên hai cách chọn cho cùng đáp số; điều thay đổi là *lý
do*, và lý do đó mới là thứ còn đúng khi phân bố độ trễ dịch chuyển. Cách làm
đúng về lâu dài là giám sát phân bố này và điều chỉnh window theo số đo, cộng
một job full-refresh định kỳ để bắt phần đuôi ngoài P99.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 2026-08-10 model phân loại dự đoán kém hẳn. `silver_tickets` có 6.606 hàng `priority` NULL hoặc ngoài miền 1..4. Pipeline không dừng, `dbt test` vẫn xanh 9/9. |
| **Nguyên nhân** | Ba lỗi chồng lên nhau. **(a)** Macro chuẩn hoá chỉ có `try_cast(priority_raw as integer)` — nó sai theo hai hướng ngược nhau: biến nhãn chữ hợp lệ (`urgent`/`high`/`medium`/`low`) thành NULL, đồng thời cho `0`, `5`, `-1` đi qua vì chúng đúng là số dù contract quy định 1..4. **(b)** `contract.enforced: false` nên dbt không kiểm tra kiểu, và cột `priority` không có test miền giá trị nào — vì thế cả hai loại sai đều đi qua **âm thầm**, không bảng điều khiển nào đỏ. **(c)** `quarantine_tickets` còn `where false` nên không có bản ghi lỗi nào được tách ra để người trực nhìn thấy. Gốc rễ chung: pipeline **không có chỗ nào phát biểu ra kỳ vọng của mình về dữ liệu**, nên nguồn đổi cách biểu diễn mà không ai biết. |
| **Ba nhóm giá trị và cách xử lý** | **Nhóm 1** `'1' '2' '3' '4'` (6.846 bản ghi) — đúng contract cũ → **giữ nguyên**, nhưng chỉ nhận trong miền 1..4.<br>**Nhóm 2** `'urgent' 'high' 'medium' 'low'` (7.142 bản ghi) — **schema evolution**: nguồn đổi *cách biểu diễn*, ý nghĩa không đổi → **quy về số** theo tài liệu API (urgent=1, high=2, medium=3, low=4).<br>**Nhóm 3** `'P1' 'P2' 'unknown' '0' '5' '-1' '' NULL` (312 bản ghi) — dữ liệu **hỏng thật** → trả NULL và đưa vào **quarantine**. |
| **Cách khắc phục** | [dbt/macros/normalize_priority.sql](dbt/macros/normalize_priority.sql) — thay `try_cast` bằng khối `CASE` xử lý ba nhóm; viết thêm `priority_reject_reason` phân biệt bốn loại lỗi.<br>[dbt/models/silver/silver_tickets.sql](dbt/models/silver/silver_tickets.sql) — **lọc bản ghi hỏng trước, `row_number()` sau**.<br>[dbt/models/silver/quarantine_tickets.sql](dbt/models/silver/quarantine_tickets.sql) — `where {{ normalize_priority('priority_raw') }} is null`.<br>[dbt/models/silver/schema.yml](dbt/models/silver/schema.yml) — `contract.enforced: true`, thêm `not_null` + `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (đúng số kỳ vọng) · `dbt test` **11/11 pass** (bản gốc 9) · `silver_tickets.priority` sạch, 0 hàng NULL hoặc ngoài 1..4 · `silver_tickets` giữ đủ **12.480** ticket |

**Vì sao thứ tự lọc/xếp hạng quyết định số hàng.** 312 bản ghi hỏng thuộc về
312 ticket khác nhau. Nếu xếp hạng trước rồi mới lọc, ticket nào có bản ghi
*mới nhất* bị hỏng sẽ biến mất hoàn toàn khỏi Silver — mất 312 ticket, còn
12.168. Lọc trước rồi mới xếp hạng thì `row_number()` chạy trên tập đã sạch, và
ticket đó vẫn còn trạng thái hợp lệ từ lần cập nhật trước. Ta loại **bản ghi**
hỏng, không loại cả **ticket** — đúng với grain của hai bảng: quarantine là
1 hàng / 1 bản ghi CDC, Silver là 1 hàng / 1 ticket.

**Phân loại lý do bị loại** (`priority_reject_reason`), để người trực đọc log
là biết phải làm gì:

| Lý do | Số bản ghi |
|---|---|
| priority là số nhưng ngoài miền 1..4 (`0`, `5`, `-1`) | 118 |
| priority là chuỗi không nằm trong bảng quy đổi (`P1`, `P2`, `unknown`) | 116 |
| priority rỗng | 43 |
| priority NULL | 35 |
| **Tổng** | **312** |

**Vì sao contract và test phải đi cùng nhau.** Contract ràng buộc **kiểu**: bật
lên thì `priority` buộc phải là `integer`, nếu model trả về varchar là dbt dừng
ngay. Nhưng contract **không** ràng buộc **miền giá trị** — nó vẫn cho
`priority = 99` đi qua vì 99 đúng là integer. Miền giá trị là việc của
`accepted_values`. Thiếu một trong hai thì vẫn còn một lối cho dữ liệu sai lọt.

### Câu hỏi thiết kế

**Chặn ở tầng Bronze hay Silver?** Chặn ở **Silver**. Bronze phải giữ nguyên bản
gốc, kể cả bản ghi hỏng — đó là quy ước của kiến trúc ba tầng và cũng là điều
kiện để điều tra được về sau. Nếu Bronze từ chối bản ghi lỗi thì ta mất chính
cái mình cần khi có sự cố: không trả lời được "nguồn đã gửi cái gì, từ lúc nào,
bao nhiêu bản ghi", không tái hiện lại được lỗi, và không backfill được sau khi
đã sửa logic chuẩn hoá — dữ liệu đã bị vứt ở cửa. Ở đây, chính việc Bronze giữ
nguyên `priority_raw` cho phép đối chiếu `bronze_tickets_cdc` với
`silver_tickets` và xác định được mốc 08-10. Silver là tầng "đã chuẩn hoá",
nên nó mới là nơi đúng để phát biểu kỳ vọng và tách bản ghi không thoả.

**Vì sao không để `dbt test` fail và dừng cả DAG?** Vì tỷ lệ: 312 bản ghi hỏng
trên tổng số 14.300 bản ghi CDC — khoảng 2,2% — và chúng không có quyền chặn
12.480 ticket, 130.683 event và 31.200 chunk hoàn toàn bình thường đến tay
người dùng. Dừng DAG biến một sự cố *cục bộ về chất lượng dữ liệu* thành một sự
cố *toàn hệ thống về tính sẵn sàng*: index RAG đứng yên, agent định tuyến không
có đặc trưng mới, trong khi vấn đề thật chỉ nằm ở một cột của một nguồn. Cách
làm đúng là **tách và chạy tiếp**: bản ghi lỗi rơi vào `quarantine_tickets` —
một hàng đợi có tên, có lý do bị loại, có thể đếm và cảnh báo khi vượt ngưỡng —
còn phần dữ liệu tốt vẫn được phục vụ. Dừng dây chuyền chỉ xứng đáng khi lỗi
lan ra toàn bộ, ví dụ nguồn gửi 100% bản ghi sai kiểu; khi đó chính con số hàng
đợi quarantine là tín hiệu để dừng.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md — làm cả A và B

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard load mất 38 giây, ba tháng trước chỉ 2 giây, không ai sửa dòng code nào. Đo được: **5.000.000 rows scanned** trên **5.000 file** cho một tập chỉ có 130.683 hàng. |
| **Nguyên nhân** | Hai vấn đề chồng lên nhau, cả hai đều thuộc *storage layout* chứ không thuộc câu truy vấn. **(a) Small-file problem:** 130.683 hàng nằm rải trên 5.000 file, trung bình 26 hàng/file. DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, nên một file 26 hàng vẫn tốn công quét tương đương ~1.000 hàng — 5.000 file tí hon thành 5.000.000 đơn vị công quét. **(b) Không có gì để bỏ qua file:** dataset không partition nên đường dẫn không mang thông tin nào của bộ lọc, và điều kiện lọc ngày lại bọc cột trong hàm — `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` — nên engine không so được với tên thư mục hay với min/max của row group. Nó buộc phải mở và giải mã **toàn bộ** 5.000 file rồi mới biết file nào có ích. Điều gì đã đổi sau ba tháng: số file cứ tăng dần theo mỗi lượt ghi, còn câu truy vấn thì đứng yên. |
| **Cách khắc phục** | [tools/compact.py](tools/compact.py) — `COPY ... TO` ghi lại dataset: `PARTITION_BY (event_date)`, `ORDER BY customer_name, event_time`, `ROW_GROUP_SIZE 1024`; có `assert` số hàng trước/sau bằng nhau.<br>[queries/dashboard.sql](queries/dashboard.sql) — trỏ vào `data/gold_events_v2/**/*.parquet` với `hive_partitioning = true`, và viết lại điều kiện thành vị từ sargable `event_date = date '2026-08-09'` (cột đứng một mình một vế). |
| **Bằng chứng** | rows scanned **5.000.000 → 9.324** (giảm **536,3×**, cần ≥ 10×) · files **5.000 → 14** · result hash **4379e4c5d9f3 không đổi** · thời gian ~20,7 ms |

**Ba quyết định của `compact.py`:**

- **Partition theo `event_date`, không theo `customer_name`.** Truy vấn lọc theo
  hai cột nhưng chỉ một cột được lên đường dẫn, vì mỗi cột partition nhân số thư
  mục lên. `event_date` có 14 giá trị → 14 thư mục, mỗi thư mục ~9.335 hàng:
  engine đọc tên thư mục là loại được 13/14 dataset trước khi mở bất kỳ file nào.
  `customer_name` có **650** giá trị → 650 thư mục ~200 hàng, tức tái tạo lại
  đúng small-file problem đang phải sửa.
- **`ORDER BY customer_name, event_time`.** Thống kê min/max của row group chỉ
  có ích khi các hàng cùng một khách nằm liền nhau; thứ tự ngẫu nhiên thì row
  group nào cũng chứa cả `ACME` lẫn `Cust_0650`, min/max phủ toàn bảng chữ cái
  và không loại được gì.
- **`ROW_GROUP_SIZE 1024`.** Mặc định 122.880 hàng gói trọn cả ngày (9.335 hàng)
  vào **một** row group, min/max của nó phủ toàn bộ 650 khách → vô dụng. 1024
  chia mỗi ngày thành ~10 row group (DuckDB làm tròn lên bội của vector size,
  thực tế ra 2.048 hàng/group), mỗi group chỉ phủ một dải hẹp tên khách.

*Ghi nhận trung thực về phần đóng góp:* toàn bộ mức giảm đo được đến từ
**partition pruning + gom file** — 9.324 rows scanned xấp xỉ đúng số hàng của
một partition ngày (9.382). Việc sắp xếp có tạo ra min/max chặt cho từng row
group (kiểm bằng `parquet_metadata`: group 1 có min = max = `ACME`), nhưng
trong phép đo này DuckDB chỉ bỏ qua được row group đầu tiên. Lý do: `COPY` với
`PARTITION_BY` gom hàng theo partition khi ghi nên không giữ trọn vẹn thứ tự
sắp xếp toàn cục. Muốn khai thác hết tầng row group thì phải ghi từng partition
bằng một câu `COPY` riêng — không làm ở đây vì mục tiêu 10× đã vượt xa.

### Bài B — Consumer bị kill giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test`: chạy một mạch được 20.000 hàng; bị giết ở lô 7 rồi khởi động lại chỉ còn **19.500 hàng — mất 500**, không trùng hàng nào. |
| **Nguyên nhân** | Consumer đang ở ngữ nghĩa **at-most-once**: `consume()` gọi `consumer.commit()` **trước** `write_batch()`. Chết ở giữa hai lệnh đó thì offset đã dịch qua một lô **chưa hề được ghi**; lần khởi động lại đọc tiếp từ offset mới, và lô đó mất vĩnh viễn — không còn dấu vết nào để biết mà đọc lại. Đúng 500 hàng = một batch. |
| **Cách khắc phục** | [ingest/consumer.py](ingest/consumer.py) — hai hạng mục, thiếu một là chưa đủ. **(a)** Đảo thứ tự thành ghi trước, commit offset sau → chuyển sang **at-least-once**: chết ở giữa thì offset chưa dịch, khởi động lại đọc lại đúng lô đó. **(b)** Biến phép ghi thành idempotent: thêm `primary key` cho `event_id` trong `DDL` (DuckDB chỉ chấp nhận `ON CONFLICT` khi cột khoá có ràng buộc), và đổi `INSERT` thuần thành `insert ... on conflict (event_id) do update set ...`. |
| **Bằng chứng** | trước: 19.500 hàng, mất 500 · sau: **20.000 hàng / 20.000 event_id khác nhau**, C == A → `BÀI MỞ RỘNG B: ĐẠT ✓`. Lần chạy sau khi sửa, tiến trình chết sớm hơn một lô (offset committed 3.000 thay vì 3.500) đúng như dự đoán, vì commit giờ đứng sau điểm crash. |

**Vì sao chỉ đảo thứ tự là chưa đủ.** Đảo thứ tự chuyển bài toán từ *mất dữ
liệu* sang *trùng dữ liệu*: lô 7 đã ghi xong rồi mới chết, khởi động lại đọc
lại chính lô đó và `INSERT` thuần sẽ tạo thêm 500 hàng nữa. Ta đổi một lỗi
không sửa được (mất hẳn) lấy một lỗi sửa được (trùng) — và trung hoà nó bằng
phép ghi idempotent. **Exactly-once không tồn tại ở tầng giao vận**; thứ chọn
được là at-least-once cộng một phép ghi idempotent, và kết quả *quan sát được*
thì tương đương exactly-once.

**`DO UPDATE` khác `DO NOTHING` ở đâu.** Với message phát lại **y hệt** thì hai
cách cho cùng kết quả. Khác biệt chỉ lộ ra khi message được phát lại với **nội
dung đã đổi** — nguồn sửa lại một event rồi gửi lại cùng `event_id`: `DO UPDATE`
lấy bản mới nhất (last-write-wins), `DO NOTHING` giữ mãi bản đầu tiên và **im
lặng** bỏ qua bản sửa, tạo ra sai lệch không có log nào ghi lại. Tôi chọn
`DO UPDATE` vì nó đúng ở cả hai trường hợp; `DO NOTHING` chỉ nên dùng khi
message được bảo đảm là bất biến theo khoá.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lượt liên tiếp và so số hàng. Với mọi model `incremental`, đọc `config()` trước khi đọc SQL: thiếu `unique_key` nghĩa là câu lệnh ghi đang là `INSERT`, và bảng sẽ phình mỗi lần ai đó chạy lại. |
| 2 | Đo phân bố `_ingested_at - event_time` của nguồn trước khi tin vào bất kỳ watermark nào. Watermark theo *thời điểm sự kiện* mà dữ liệu lại tới muộn thì hàng bị bỏ qua sẽ mất vĩnh viễn — và bảng vẫn "ổn định", nên không có cảnh báo nào nổi lên. |
| 3 | Xem pipeline có phát biểu kỳ vọng của mình ở đâu không: contract có bật không, cột khoá có test miền giá trị không, có bảng quarantine nào không rỗng không. Không có ba thứ đó thì nguồn đổi format sẽ đi qua âm thầm, và người đầu tiên phát hiện sẽ là người dùng mô hình chứ không phải data engineer. |
| A | Với một truy vấn chậm dần theo thời gian mà không ai sửa code, nhìn *storage layout* trước khi nhìn câu SQL: đếm số file và số hàng mỗi file, rồi đối chiếu các cột trong `WHERE` với thông tin có trên đường dẫn. Cột bị bọc trong hàm là dấu hiệu engine không thể lọc sớm. |
| B | Với mọi consumer, đọc đúng thứ tự ba thao tác *ghi — commit offset — điểm có thể chết*. Thứ tự đó quyết định ngữ nghĩa giao vận, và một phép ghi không idempotent thì không có thứ tự nào là an toàn. |
