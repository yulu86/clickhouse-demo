create database mytest;

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

-- CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT

truncate tbl_counter;
truncate tbl_counter_agg1;

drop table mv_counter_agg1;

insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:05:00', 10, 20),
(1, '2023/02/03 20:10:00', 20, 30),
(1, '2023/02/03 20:15:00', 30, 40),
(2, '2023/02/03 20:05:00', 10, 20),
(2, '2023/02/03 20:10:00', 20, 30),
(2, '2023/02/03 20:15:00', 30, 40);

insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:10:00', 100, 100),
(2, '2023/02/03 20:10:00', 100, 100);


insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:05:00', 100, 100),
(2, '2023/02/03 20:05:00', 100, 100);

insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:05:00', 120, 100),
(2, '2023/02/03 20:05:00', 120, 100);

-- 表1 去重
CREATE TABLE tbl_counter_agg1 (
  device Int32,
  time DateTime,
  year UInt16,
  month UInt8,
  day UInt8,
  hour UInt8,  
  result_time DateTime,
  count_state AggregateFunction(count, Int32),
  counter1_state AggregateFunction(argMax, Float32, DateTime),
  last_counter1 Float32,
  counter2_state AggregateFunction(argMax, Float32, DateTime),
  last_counter2 Float32
)
ENGINE = AggregatingMergeTree() 
ORDER BY (device, time)

-- MV1 去重
CREATE MATERIALIZED VIEW mv_counter_agg1 TO tbl_counter_agg1
AS 
SELECT source.device,
  source.time,
  toYear(source.time) as year,
  toMonth(source.time) as month,
  toDayOfMonth(source.time) as day,
  toHour(source.time) as hour,
  toStartOfHour(source.time) as result_time,
  countState(source.device) as count_state,
  argMaxState(source.counter1, source.insert_time) as counter1_state,
  max(dest.counter1) as last_counter1,
  argMaxState(source.counter2, source.insert_time) as counter2_state,
  max(dest.counter2) as last_counter2
FROM tbl_counter source LEFT JOIN (
  SELECT device,
    time,
    argMaxMerge(counter1_state) as counter1,
    argMaxMerge(counter2_state) as counter2
  FROM tbl_counter_agg1
  GROUP BY device, time, year, month, day, hour
) dest
ON source.device = dest.device
  AND source.time = dest.time
GROUP BY device, time;


SELECT * FROM tbl_counter_agg1 ORDER BY device, time

-- 查询结果
SELECT device,
  time,
  year,
  month,
  day,
  hour,
  countMerge(count_state) as total_count,
  argMaxMerge(counter1_state) as counter1,
  argMaxMerge(counter2_state) as counter2,
  max(last_counter1) as last_counter1,
  max(last_counter2) as last_counter2
FROM tbl_counter_agg1
GROUP BY device, time, year, month, day, hour
ORDER BY device, time


-- 表2 聚合
CREATE TABLE tbl_counter_agg2 (
  device Int32,
  year UInt16,
  month UInt8,
  day UInt8,
  hour UInt8,  
  result_time DateTime,
  count_state AggregateFunction(count, Int32),
  counter1_sum_state AggregateFunction(sum, Float64),
  counter1_max_state AggregateFunction(max, Float64),
  counter1_min_state AggregateFunction(max, Float64),
  counter1_avg_state AggregateFunction(avg, Float64),
  last_counter1 Float32
)
ENGINE = AggregatingMergeTree() 
ORDER BY (device, year, month, day, hour, result_time)

-- MV2 聚合
CREATE MATERIALIZED VIEW mv_counter_agg2 TO tbl_counter_agg2
AS 
SELECT device,
  year,
  month,
  day,
  hour,
  result_time,
  countState(device) as count_state,
  sumState(counter1 - last_counter1) as counter1_sum_state
FROM (
  SELECT device,
    year,
    month,
    day,
    hour,
    result_time,
    countMerge(count_state) as total_count,
    argMaxMerge(counter1_state) as counter1,
    argMaxMerge(counter2_state) as counter2,
    max(last_counter1) as last_counter1,
    max(last_counter2) as last_counter2
  FROM tbl_counter_agg1
  GROUP BY device, time, year, month, day, hour, result_time
)
GROUP BY device, year, month, day, hour, result_time


-- 查询
SELECT * FROM tbl_counter_agg2
ORDER BY device, year, month, day, hour, result_time


SELECT device,
  year,
  month,
  day,
  hour,
  result_time,
  count(device) as count,
  countMerge(count_state) as total_count,
  sumMerge(counter1_sum_state) as counter1_sum,
  (counter1_sum / count) as counter1_avg
FROM tbl_counter_agg2
GROUP BY device, year, month, day, hour, result_time
ORDER BY device, year, month, day, hour, result_time