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

drop table tbl_counter_agg2;
drop table mv_counter2;

truncate tbl_counter;
truncate tbl_counter_agg;
truncate tbl_counter_agg2;

insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:05:00', 10, 20),
(1, '2023/02/03 20:10:00', 20, 30),
(1, '2023/02/03 20:15:00', 30, 40),
(2, '2023/02/03 20:05:00', 10, 20),
(2, '2023/02/03 20:10:00', 20, 30),
(2, '2023/02/03 20:15:00', 30, 40);

insert into tbl_counter(device, time, counter1, counter2) values 
(1, '2023/02/03 20:10:00', 100, 100),
(2, '2023/02/03 20:10:00', 100, 100),


select * from tbl_counter;
select * from tbl_counter_agg;
select * from tbl_counter_agg2;

select toTypeName(result_time) from mv_counter_agg1 limit 1


AggregateFunction(max, Tuple(DateTime, Float32)) 



-- Step1. 去重
CREATE TABLE tbl_counter_agg1 (
  device Int32,
  time DateTime,
  year UInt16,
  month UInt8,
  day UInt8,
  hour UInt8,  
  result_time DateTime,
  currrent_count Int32,
  count_state AggregateFunction(count, Int32),
  counter1_state AggregateFunction(argMax, DateTime, Float32),
  counter2_state AggregateFunction(argMax, DateTime, Float32)
)
ENGINE = AggregatingMergeTree() 
ORDER BY (device, time)


-- MV1
CREATE MATERIALIZED VIEW mv_counter_agg1 TO tbl_counter_agg1
AS 
SELECT device,
  time,
  toYear(time) as year,
  toMonth(time) as month,
  toDayOfMonth(time) as day,
  toHour(time) as hour,
  toStartOfHour(time) as result_time,
  count(device) as currrent_count,
  countState(device) as count_state,
  argMaxState(insert_time, counter1) as counter1_state,
  argMaxState(insert_time, counter2) as counter2_state
FROM tbl_counter
GROUP BY device, time;

select * from tbl_counter_agg1




SELECT device,
  time,
  year,
  month,
  day,  
  hour,
  result_time,
  sum(currrent_count),
  max(currrent_count),
  countMerge(count_state) as count,
  argMaxMerge(counter1_state) as couter1,
  argMaxMerge(counter2_state) as couter2
FROM tbl_counter_agg1
GROUP BY device, time, year, month, day, hour, result_time


select * from tbl_counter order by device, time, insert_time desc


-- MV2
CREATE MATERIALIZED VIEW mv_counter_agg2 
ENGINE = AggregatingMergeTree() 
ORDER BY (device, result_time, year, month, day, hour)
AS 
SELECT device,
  year,
  month,
  day,  
  hour,
  result_time,
  maxIfState(counter1_deduplicate, ) as counter1,
  maxState(counter2_deduplicate) as counter2,

FROM
  (SELECT device,
    year,
    month,
    day,  
    hour,
    result_time,
    time,
    tupleElement(maxMerge(counter1), 2) as counter1_deduplicate,
    tupleElement(maxMerge(counter2), 2) as counter2_deduplicate,
    tupleElement(maxMerge(counter1), 1) as insert_time,
    maxMerge(max_insert_time) as max_insert_time
  FROM tlb_counter_agg1
  GROUP BY device, year, month, day, hour, result_time, time
  ORDER BY device, year, month, day, hour, result_time, time, insert_time desc
  )
GROUP BY device, year, month, day, hour, result_time;

select device,
  year,
  month,
  day,  
  hour,
  result_time,
  maxMerge(counter1) as counter1,
  maxMerge(counter2) as counter2
from 
  (SELECT device,
    year,
    month,
    day,  
    hour,
    result_time,
    maxState(counter1_deduplicate) as counter1,
    maxState(counter2_deduplicate) as counter2
  FROM
    (SELECT 
      device,
      year,
      month,
      day,  
      hour,
      result_time,
      tupleElement(maxMerge(counter1), 2) as counter1_deduplicate,
      tupleElement(maxMerge(counter2), 2) as counter2_deduplicate
    FROM mv_counter_agg1
    GROUP BY device, year, month, day, hour, result_time, time
    )
  GROUP BY device, year, month, day, hour, result_time
  )
GROUP BY device, year, month, day, hour, result_time;






SELECT
  device,
  time,
  tupleElement(maxMerge(counter1), 2) as counter1,
  tupleElement(maxMerge(counter2), 2) as counter2
FROM mv_counter2
GROUP BY device, time;


SELECT * FROM mv_counter2


select 
  device,
  time,
  maxMerge(counter1) as counter1,
  maxMerge(counter12) as counter2
from tbl_counter
group by device, time



select 
  device,
  time,
  max(tuple(insert_time, counter1)) as counter1,
  max(tuple(insert_time, counter2)) as counter2
from tbl_counter
group by device, time