-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- Model này materialized = 'table': dựng lại toàn bộ mỗi lần chạy, nên luôn
-- ổn định.
--
-- Phần xử lý CDC dưới đây đã đúng và không cần sửa:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất  -> _rn = 1
--   * ticket có op = 'd' bị loại khỏi Silver
--
-- ===========================================================================
-- NHIỆM VỤ 3 — phần 2/3
-- ===========================================================================
-- Giá trị `priority` được chuẩn hoá bằng macro
-- `dbt/macros/normalize_priority.sql` — sửa logic quy đổi ở đó, không sửa ở
-- đây. Việc còn lại của file này là **loại bỏ những bản ghi không chuẩn hoá
-- được** ra khỏi Silver.
--
-- ⚠️ THỨ TỰ QUYẾT ĐỊNH SỐ HÀNG CỦA BẢNG.
--
--    Hiện tại: xếp hạng (row_number) trước, rồi mới chọn.
--    Nếu bạn chỉ thêm điều kiện lọc vào cuối, ticket nào có bản ghi MỚI NHẤT
--    bị hỏng sẽ biến mất hoàn toàn khỏi Silver — số ticket tụt từ 12.480
--    xuống 12.168, và gold_training_set hụt theo.
--
--    Đúng phải là: LỌC bỏ bản ghi hỏng trước → SAU ĐÓ mới xếp hạng.
--    Bạn loại BẢN GHI hỏng, không loại cả TICKET: ticket đó vẫn còn trạng
--    thái hợp lệ từ lần cập nhật trước.
--
--    `make verify` có một dòng riêng ("silver_tickets giữ đủ 12.480 ticket")
--    để bắt đúng lỗi này.
--
-- Xong file này thì sang models/silver/quarantine_tickets.sql (phần 3/3).
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

-- LỌC TRƯỚC: bỏ những BẢN GHI CDC không chuẩn hoá được (chúng nằm ở
-- quarantine_tickets). Ticket vẫn còn nguyên trong Silver với trạng thái hợp lệ
-- gần nhất — ta loại bản ghi hỏng, không loại cả ticket.
with valid as (

    select
        *,
        {{ normalize_priority('priority_raw') }}             as priority_clean
    from {{ source('bronze', 'bronze_tickets_cdc') }}
    where {{ normalize_priority('priority_raw') }} is not null

),

-- XẾP HẠNG SAU: chỉ xếp hạng trên tập bản ghi đã hợp lệ.
ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority_clean                                           as priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
