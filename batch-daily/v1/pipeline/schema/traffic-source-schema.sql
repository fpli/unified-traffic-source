--------------------------------------------------------------------------------
-- Traffic source detection in unified session - batch table schema
-- 
-- naming convention: 
-- * working tables are prefixed with ubi_w.tbd_ts_
-- * tbd: tracking behavior data
-- * ts: traffic source
-- 
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=d04cb2f4-343e-49e7-8061-019e5d11e41a
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_session_snapshot
-- Snapshot of ubi_t.unified_session
-- We make a copy at the beginning of traffic source detection pipeline.
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_session_snapshot;
-- CREATE TABLE ubi_w.tbd_ts_unified_session_snapshot (
--   guid STRING,
--   global_session_id STRING,
--   abs_start_timestamp BIGINT,
--   abs_end_timestamp BIGINT,
--   session_start_dt BIGINT,
--   traffic_source STRING,
--   bot_flag STRUCT<surface: ARRAY<INT>, ubi: ARRAY<INT>, utp: ARRAY<INT>>,
--   others MAP<STRING, STRING>,
--   bot_type STRING,
--   dt STRING,
--   data_type STRING,
--   session_type STRING)
-- USING parquet
-- PARTITIONED BY (dt, data_type, session_type)
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_session_snapshot';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_surface_event
-- Valid surface events in unified sessions
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_surface_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_surface_event (
--   global_session_id	string,
--   source string,
--   source_session_skey bigint,
--   guid string,
--   session_id bigint,
--   page_id string,
--   page_name	varchar(255),
--   event_timestamp bigint,
--   referer string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_surface_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_ubi_event
-- Valid UBI (Sojourner) events in unified sessions
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_ubi_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_ubi_event (
--   global_session_id	string,
--   source string,
--   source_session_skey bigint,
--   guid string,
--   session_skey bigint,
--   page_id int,
--   page_name	varchar(255),
--   event_timestamp timestamp,
--   referer string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_ubi_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_first_valid_event
-- First valid event in each unified session
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_first_valid_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_first_valid_event (
--   guid string,
--   global_session_id string,
--   source string,
--   page_id string,
--   page_name varchar(255),
--   event_timestamp bigint,
--   referer string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_first_valid_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_utp_event
-- UTP (Chocolate) events in unified sessions
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_utp_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_utp_event (
--   global_session_id string,
--   source_session_skey bigint,
--   GUID string,
--   SESSION_SKEY bigint,
--   EVENT_TIMESTAMP bigint,
--   chnl string,
--   rotid string,
--   url string,
--   rk int,
--   MPX_CHNL_ID smallint)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_utp_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_first_utp_event
-- First UTP events in each unified session
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_first_utp_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_first_utp_event (
--   global_session_id string,
--   guid string,
--   source_session_skey bigint,
--   session_skey bigint,
--   event_timestamp bigint,
--   chnl string,
--   rotid string,
--   url string,
--   mpx_chnl_id smallint)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_first_utp_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_imbd_event
-- IMBD events in unified sessions
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_imbd_event;
-- CREATE TABLE if not exists ubi_w.tbd_ts_imbd_event (
--   global_session_id string,
--   source_session_skey bigint,
--   GUID string,
--   SESSION_SKEY bigint,
--   EVENT_TIMESTAMP bigint,
--   chnl string,
--   mppid string,
--   rk int)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_imbd_event';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_ts_1
-- Traffic source detected by using UTP and IMBD events
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_ts_1;
-- CREATE TABLE if not exists ubi_w.tbd_ts_unified_ts_1 (
--   global_session_id string,
--   guid	string,
--   session_start_timestamp bigint,
--   event_timestamp bigint,
--   chnl string,
--   rotid string,
--   mpx_chnl_id smallint,
--   url string,
--   mppid string,
--   page_name varchar(255),
--   referer string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_ts_1';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_ts_2
-- Traffic source detected by using referrer
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_ts_2;
-- CREATE TABLE if not exists ubi_w.tbd_ts_unified_ts_2 (
--   global_session_id string,
--   guid string,
--   session_start_timestamp bigint,
--   event_timestamp bigint,
--   rotid string,
--   mpx_chnl_id smallint,
--   url string,
--   page_name varchar(255),
--   referer string,
--   chnl string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_ts_2';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_traffic_source
-- Generate final traffic source based on ubi_w.tbd_ts_unified_ts_2
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_traffic_source;
-- CREATE TABLE if not exists ubi_w.tbd_ts_unified_traffic_source (
--   global_session_id	string,
--   guid string,
--   traffic_source string,
--   session_start_timestamp bigint,
--   event_timestamp bigint,
--   rotid string,
--   mpx_chnl_id smallint,
--   url string,
--   page_name varchar(255),
--   referer string)
-- USING parquet
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_traffic_source';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_session_with_ts_snapshot
-- Snapshot table of ubi_t.unified_session before insert into the target table
-- this should be replace by ubi_t.unified_session_swap
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_session_with_ts_snapshot;
-- CREATE TABLE ubi_w.tbd_ts_unified_session_with_ts_snapshot (
--   guid STRING,
--   global_session_id STRING,
--   abs_start_timestamp BIGINT,
--   abs_end_timestamp BIGINT,
--   session_start_dt BIGINT,
--   traffic_source MAP<STRING, STRING>,
--   bot_flag STRUCT<surface: ARRAY<INT>, ubi: ARRAY<INT>, utp: ARRAY<INT>>,
--   others MAP<STRING, STRING>,
--   bot_type STRING,
--   dt STRING,
--   data_type STRING,
--   session_type STRING)
-- USING parquet
-- PARTITIONED BY (dt, data_type, session_type)
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_session_with_ts_snapshot';

--------------------------------------------------------------------------------
-- [Working Table] ubi_w.tbd_ts_unified_session_with_ts
-- We use this table as backup for debug
-- Write to this table before write to ubi_t.unified_session
--------------------------------------------------------------------------------
-- drop table if exists ubi_w.tbd_ts_unified_session_with_ts;
-- CREATE TABLE ubi_w.tbd_ts_unified_session_with_ts (
--   guid STRING,
--   global_session_id STRING,
--   abs_start_timestamp BIGINT,
--   abs_end_timestamp BIGINT,
--   session_start_dt BIGINT,
--   traffic_source MAP<STRING, STRING>,
--   bot_flag STRUCT<surface: ARRAY<INT>, ubi: ARRAY<INT>, utp: ARRAY<INT>>,
--   others MAP<STRING, STRING>,
--   bot_type STRING,
--   dt STRING,
--   data_type STRING,
--   session_type STRING)
-- USING parquet
-- PARTITIONED BY (dt, data_type, session_type)
-- LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/tbd_ts_unified_session_with_ts';

--------------------------------------------------------------------------------
-- [Prod Table] ubi_t.unified_session_swap
--------------------------------------------------------------------------------
-- refresh table ubi_t.unified_session_swap;
-- show partitions ubi_t.unified_session_swap;
-- show create table ubi_t.unified_session_swap;

-- INSERT OVERWRITE TABLE ubi_t.unified_session_swap
-- SELECT *
-- FROM ubi_t.unified_session a
-- WHERE ((a.dt = '20230228' AND a.session_type = 'sameday') 
--   or (a.dt = '20230227' AND a.session_type = 'crossday'));

-- desc database extended ubi_t;
-- SHOW TABLE EXTENDED IN ubi_t LIKE 'unified_session_swap';
-- Just add new partitions in default table location. This will NOT remove any partition metadata that has no corresponding directory.
-- ALTER TABLE ubi_t.unified_session_swap RECOVER PARTITIONS;
-- This will drop all sub partitions
-- ALTER TABLE ubi_t.unified_session_swap DROP IF EXISTS PARTITION (dt='20230228');

-- SHOW TABLE EXTENDED IN ubi_t LIKE 'unified_session_swap' PARTITION (dt='20230228', data_type='major', session_type='sameday');
-- SHOW TABLE EXTENDED IN ubi_t LIKE 'unified_session_swap' PARTITION (dt='20230228', data_type='patch', session_type='sameday');
-- SHOW TABLE EXTENDED IN ubi_t LIKE 'unified_session_swap' PARTITION (dt='20230227', data_type='major', session_type='crossday');
-- SHOW TABLE EXTENDED IN ubi_t LIKE 'unified_session_swap' PARTITION (dt='20230227', data_type='patch', session_type='crossday');

-- This adds metadata and also creates empty directories in default table location according to partition spec.
-- ALTER TABLE ubi_t.unified_session_swap ADD IF NOT EXISTS PARTITION (dt='20230228', data_type='major', session_type='sameday');
-- Point to other directories
-- ALTER TABLE ubi_t.unified_session_swap 
-- PARTITION (dt='20230228', data_type='major', session_type='sameday') 
-- SET LOCATION 'viewfs://apollo-rno/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=20230228/data_type=major/session_type=sameday';

-- ALTER TABLE ubi_t.unified_session_swap ADD IF NOT EXISTS PARTITION (dt='20230228', data_type='patch', session_type='sameday');
-- ALTER TABLE ubi_t.unified_session_swap 
-- PARTITION (dt='20230228', data_type='patch', session_type='sameday') 
-- SET LOCATION 'viewfs://apollo-rno/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=20230228/data_type=patch/session_type=sameday';

-- show create table ubi_t.unified_session_swap;

-- CREATE TABLE ubi_w.tbd_ts_unified_session_v1_no_trfc_src (
--   `guid` STRING,
--   `global_session_id` STRING,
--   `abs_start_timestamp` BIGINT,
--   `abs_end_timestamp` BIGINT,
--   `session_start_dt` BIGINT,
--   `traffic_source` MAP<STRING, STRING>,
--   `bot_flag` STRUCT<`surface`: ARRAY<INT>, `ubi`: ARRAY<INT>, `utp`: ARRAY<INT>>,
--   `others` MAP<STRING, STRING>,
--   `bot_type` STRING,
--   `dt` STRING,
--   `data_type` STRING,
--   `session_type` STRING)
-- USING parquet
-- OPTIONS (
--   `compression` 'snappy')
-- PARTITIONED BY (dt, data_type, session_type)
-- LOCATION 'viewfs://apollo-rno/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src';

-- ALTER TABLE ubi_w.tbd_ts_unified_session_v1_no_trfc_src RECOVER PARTITIONS;
