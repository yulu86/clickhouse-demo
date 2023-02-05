use mytest;

-- 原始表
CREATE TABLE tbl_counter(
device Int32,
time DateTime,
insert_time DateTime default now(),
counter1 Float(32),
counter2 Float(32)
) ENGINE = ReplacingMergeTree() 
ORDER BY (device, time);