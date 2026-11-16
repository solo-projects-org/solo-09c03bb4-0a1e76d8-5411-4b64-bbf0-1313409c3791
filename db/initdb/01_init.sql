-- 震例表：震例号、UTC 发震时刻、纬度、经度、震级
CREATE TABLE seismic_events (
    event_id    text PRIMARY KEY,           -- 震例号
    origin_time timestamptz NOT NULL,       -- UTC 发震时刻
    latitude    numeric(6,3) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude   numeric(7,3) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    magnitude   numeric(3,1) NOT NULL       -- 震级
);

-- 让文本协议输出保持 ISO 风格与 UTC（初始装载与 wal2json 都走文本输出）
ALTER DATABASE quakes SET datestyle = 'ISO, MDY';
ALTER DATABASE quakes SET timezone = 'UTC';

-- 样库：30000 条历史震例，发震时刻分布于 2020-01-01 之后约 2000 天
INSERT INTO seismic_events (event_id, origin_time, latitude, longitude, magnitude)
SELECT 'EQH-' || lpad(g::text, 6, '0'),
       timestamptz '2020-01-01 00:00:00+00'
         + (g * interval '96 minutes')
         + (random() * interval '60 minutes'),
       round((18 + random() * 36)::numeric, 3),
       round((73 + random() * 62)::numeric, 3),
       round((2.5 + random() * random() * 6.8)::numeric, 1)
FROM generate_series(1, 30000) AS g;

CREATE INDEX seismic_events_origin_time_idx ON seismic_events (origin_time);

-- 震级更正回执：数据员从网页写回源目录的每一笔更正（含被拒绝的）都在此留痕。
-- 该表是“固定请求号”幂等的根基：request_id 相同且内容相同的重试直接返回原记录，
-- 不会重复改写；request_id 相同但内容不同的提交被拒绝。
-- 回执与震级更正在同一源事务提交，经逻辑复制与更正同笔落入镜像，
-- 采集端据此确认“已采回”（而不是凭震级相等或 WAL 位置推断）。
CREATE TABLE corrections (
    request_id    text PRIMARY KEY,         -- 固定请求号（幂等键），网页生成
    event_id      text NOT NULL,            -- 目标震例号
    new_magnitude numeric(3,1) NOT NULL,    -- 更正后震级
    expected      text NOT NULL,            -- JSON：提交时网页所见的五个字段（整行核对基准）
    outcome       text NOT NULL             -- applied=已写入；rejected_*=已拒绝（留痕）
                  CHECK (outcome IN ('applied', 'rejected_mismatch', 'rejected_deleted')),
    source_row    text,                     -- JSON：提交时源库实际整行（拒绝时留存备查）
    submitted_at  timestamptz NOT NULL DEFAULT now()
);
