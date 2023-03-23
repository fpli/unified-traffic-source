--------------------------------------------------------------------------------
-- Pipeline operation cookbook
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=99bca069-dd21-46d1-9c78-5eb82e7d7095
--------------------------------------------------------------------------------

-- %refresh_var(${UOW_FROM_DATE});
%refresh_var(${dt_1});
%refresh_var(${dt_1_formated});
%refresh_var(${dt_2});
%refresh_var(${dt_2_formated});

--------------------------------------------------------------------------------
-- 1. Rollback and rerun the pipeline without error = restatement
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 1.1 check data volume
--    ubi_w.uts_unified_session_v1_no_trfc_src: row count = ubi_t.unified_session
--    ubi_w.uts_unified_session_v1_no_trfc_src: no row with traffic source
--------------------------------------------------------------------------------

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	495234
-- 20230318	patch	crossday	8580
-- 20230319	major	sameday	6081886395
-- 20230319	patch	sameday	971968
SELECT dt, data_type, session_type, count(1)
FROM ubi_w.uts_unified_session_v1_no_trfc_src a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday')
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
group by dt, data_type, session_type;

-- empty
SELECT dt, data_type, session_type, count(1)
FROM ubi_w.uts_unified_session_v1_no_trfc_src a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday')
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	495234
-- 20230318	patch	crossday	8580
-- 20230319	major	sameday	6081886395
-- 20230319	patch	sameday	971968
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday')
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	297661
-- 20230318	patch	crossday	8442
-- 20230319	major	sameday	122538894
-- 20230319	patch	sameday	66308
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday')
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

--------------------------------------------------------------------------------
-- 1.2 Recover unified session with ubi_w.uts_unified_session_v1_no_trfc_src
-- we use static partition overwrite mode because it's safer.
--------------------------------------------------------------------------------

ALTER TABLE ubi_w.uts_unified_session_v1_no_trfc_src RECOVER PARTITIONS;
show partitions ubi_w.uts_unified_session_v1_no_trfc_src;

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_2}', data_type = 'major', session_type = 'crossday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_unified_session_v1_no_trfc_src
WHERE dt = '${dt_2}' and data_type = 'major' AND session_type = 'crossday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_2}', data_type = 'patch', session_type = 'crossday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_unified_session_v1_no_trfc_src
WHERE dt = '${dt_2}' and data_type = 'patch' AND session_type = 'crossday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_1}', data_type = 'major', session_type = 'sameday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_unified_session_v1_no_trfc_src
WHERE dt = '${dt_1}' and data_type = 'major' AND session_type = 'sameday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_1}', data_type = 'patch', session_type = 'sameday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_unified_session_v1_no_trfc_src
WHERE dt = '${dt_1}' and data_type = 'patch' AND session_type = 'sameday';

--------------------------------------------------------------------------------
-- 1.3 check ubi_t.unified_session again
--    there should be no row with traffic source
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- 1.4 Cleanup ubi_w.uts_unified_session_v1_no_trfc_src
--------------------------------------------------------------------------------
-- Apollo CLI
-- change dt first before run the shell scripts
-- operations/bin/uni_trfc_src_clean_swap_v1.sh



--------------------------------------------------------------------------------
-- 2. Rerun upstream unified session map and rerun the pipeline
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 2.1 Cleanup ubi_w.tbd_ts_unified_session_v1_no_trfc_src
--------------------------------------------------------------------------------
-- Apollo CLI
-- change dt first before run the shell scripts
-- operations/bin/uni_trfc_src_clean_swap_v1.sh



--------------------------------------------------------------------------------
-- 3. Rollback and rerun the pipeline with error
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 3.1 check data volume
-- ubi_w.uts_v2_unified_session_copy: this is backup of ubi_t.unified_session
--------------------------------------------------------------------------------

refresh table ubi_w.uts_v2_unified_session_copy;
show partitions ubi_w.uts_v2_unified_session_copy;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	495234
-- 20230318	patch	crossday	8580
-- 20230319	major	sameday	6081886395
-- 20230319	patch	sameday	971968
select dt, data_type, session_type, count(1)
from ubi_w.uts_v2_unified_session_copy
where ((dt = '${dt_1}' and session_type = 'sameday')
  or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

-- empty
select dt, data_type, session_type, count(1)
from ubi_w.uts_v2_unified_session_copy
where ((dt = '${dt_1}' and session_type = 'sameday')
  or (dt = '${dt_2}' and session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	495234
-- 20230318	patch	crossday	8580
-- 20230319	major	sameday	6081886395
-- 20230319	patch	sameday	971968
refresh table ubi_t.unified_session;
select dt, data_type, session_type, count(1)
from ubi_t.unified_session
where ((dt = '${dt_1}' and session_type = 'sameday')
  or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	297661
-- 20230318	patch	crossday	8442
-- 20230319	major	sameday	122538894
-- 20230319	patch	sameday	66308
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday')
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

--------------------------------------------------------------------------------
-- 3.2 Recover unified session with ubi_w.uts_v2_unified_session_copy
--     we use static partition overwrite mode because it's safer.
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_2}', data_type = 'major', session_type = 'crossday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  map() traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_v2_unified_session_copy
WHERE dt = '${dt_2}' and data_type = 'major' AND session_type = 'crossday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_2}', data_type = 'patch', session_type = 'crossday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  map() traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_v2_unified_session_copy
WHERE dt = '${dt_2}' and data_type = 'patch' AND session_type = 'crossday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_1}', data_type = 'major', session_type = 'sameday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  map() traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_v2_unified_session_copy
WHERE dt = '${dt_1}' and data_type = 'major' AND session_type = 'sameday';

INSERT OVERWRITE TABLE ubi_t.unified_session
partition (dt = '${dt_1}', data_type = 'patch', session_type = 'sameday')
SELECT
  guid,
  global_session_id,
  abs_start_timestamp,
  abs_end_timestamp,
  session_start_dt,
  map() traffic_source,
  bot_flag,
  others,
  bot_type
FROM ubi_w.uts_v2_unified_session_copy
WHERE dt = '${dt_1}' and data_type = 'patch' AND session_type = 'sameday';

--------------------------------------------------------------------------------
-- 3.3 check ubi_t.unified_session again
--    there should be no row with traffic source
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- 3.4 Cleanup ubi_w.uts_unified_session_v1_no_trfc_src
--------------------------------------------------------------------------------
-- Apollo CLI
-- change dt first before run the shell scripts
-- operations/bin/uni_trfc_src_clean_swap_v1.sh
