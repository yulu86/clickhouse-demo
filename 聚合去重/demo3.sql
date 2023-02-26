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

-- MV1 去重
CREATE MATERIALIZED VIEW mv_counter_agg1 ENGINE = AggregatingMergeTree()
 ORDER BY (device, time)
AS 
SELECT device,
  time,
  toYear(time) as year,
  toMonth(time) as month,
  toDayOfMonth(time) as day,
  toHour(time) as hour,
  toStartOfHour(time) as result_time,
  countState(device) as count_state,
  argMaxState(counter1, insert_time) as counter1_state,
  argMaxState(counter2, insert_time) as counter2_state
FROM tbl_counter
GROUP BY device, time;

SELECT * FROM mv_counter_agg1 ORDER BY device, time

SELECT device,
  time,
  year,
  month,
  day,
  hour,
  countMerge(count_state) as total_count,
  argMaxMerge(counter1_state) as count1,
  argMaxMerge(counter2_state) as count2
FROM mv_counter_agg1
GROUP BY device, time, year, month, day, hour