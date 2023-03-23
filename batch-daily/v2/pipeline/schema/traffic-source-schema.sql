--------------------------------------------------------------------------------
-- Working tables:
-- ubi_w.uts_v2_unified_session_copy
-- ubi_w.uts_v2_surface_event
-- ubi_w.uts_v2_ubi_event
-- ubi_w.uts_v2_deeplink_event
-- ubi_w.uts_v2_first_valid_event
-- ubi_w.uts_v2_utp_event
-- ubi_w.uts_v2_imbd_event
-- ubi_w.uts_v2_traffic_source_1
-- ubi_w.uts_v2_traffic_source_2
-- ubi_w.uts_v2_unified_traffic_source
-- ubi_w.uts_v2_unified_traffic_source_backup
-- ubi_w.uts_unified_session_v1_no_trfc_src
--
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=8507a3da-982d-423e-a038-10d34f2c8ac4
--------------------------------------------------------------------------------

drop table if exists ubi_w.uts_v2_unified_session_copy;
CREATE TABLE ubi_w.uts_v2_unified_session_copy (
  `guid` STRING,
  `global_session_id` STRING,
  `abs_start_timestamp` BIGINT,
  `abs_end_timestamp` BIGINT,
  `session_start_dt` BIGINT,
  `traffic_source` MAP<STRING, STRING>,
  `bot_flag` STRUCT<`surface`: ARRAY<INT>, `ubi`: ARRAY<INT>, `utp`: ARRAY<INT>>,
  `others` MAP<STRING, STRING>,
  `bot_type` STRING,
  `dt` STRING,
  `data_type` STRING,
  `session_type` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
PARTITIONED BY (dt, data_type, session_type)
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_unified_session_copy';

drop table if exists ubi_w.uts_v2_surface_event;
CREATE TABLE ubi_w.uts_v2_surface_event (
  `global_session_id` STRING,
  `source` STRING,
  `source_session_skey` BIGINT,
  `guid` STRING,
  `SESSION_ID` BIGINT,
  `PAGE_ID` STRING,
  `PAGE_NAME` STRING,
  `EVENT_TIMESTAMP` BIGINT,
  `REFERER` STRING,
  `url` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_surface_event';

drop table if exists ubi_w.uts_v2_ubi_event;
CREATE TABLE ubi_w.uts_v2_ubi_event (
  `global_session_id` STRING,
  `source` STRING,
  `source_session_skey` BIGINT,
  `guid` STRING,
  `SESSION_SKEY` BIGINT,
  `PAGE_ID` INT,
  `PAGE_NAME` STRING,
  `EVENT_TIMESTAMP` TIMESTAMP,
  `REFERER` STRING,
  `url` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_ubi_event';

drop table if exists ubi_w.uts_v2_deeplink_event;
CREATE TABLE ubi_w.uts_v2_deeplink_event (
  `global_session_id` STRING,
  `source_session_skey` BIGINT,
  `GUID` STRING,
  `SESSION_SKEY` BIGINT,
  `EVENT_TIMESTAMP` BIGINT,
  `referer` STRING,
  `rk` INT)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_deeplink_event';

drop table if exists ubi_w.uts_v2_first_valid_event;
CREATE TABLE ubi_w.uts_v2_first_valid_event (
  `guid` STRING,
  `global_session_id` STRING,
  `source` STRING,
  `PAGE_ID` STRING,
  `PAGE_NAME` STRING,
  `EVENT_TIMESTAMP` BIGINT,
  `URL` STRING,
  `REFERER` STRING,
  `deeplink_timestamp` BIGINT)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_first_valid_event';

drop table if exists ubi_w.uts_v2_utp_event;
CREATE TABLE ubi_w.uts_v2_utp_event (
  `global_session_id` STRING,
  `source_session_skey` BIGINT,
  `GUID` STRING,
  `SESSION_SKEY` BIGINT,
  `EVENT_TIMESTAMP` BIGINT,
  `chnl` STRING,
  `rotid` BIGINT,
  `url` STRING,
  `rk` INT,
  `MPX_CHNL_ID` SMALLINT)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_utp_event';

drop table if exists ubi_w.uts_v2_imbd_event;
CREATE TABLE ubi_w.uts_v2_imbd_event (
  global_session_id string,
  source_session_skey bigint,
  GUID string,
  SESSION_SKEY bigint,
  EVENT_TIMESTAMP bigint,
  chnl string,
  mppid string,
  rk int)
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_imbd_event';

drop table if exists ubi_w.uts_v2_traffic_source_1;
CREATE TABLE ubi_w.uts_v2_traffic_source_1 (
  `global_session_id` STRING,
  `guid` STRING,
  `session_start_timestamp` BIGINT,
  `event_timestamp` BIGINT,
  `chnl` STRING,
  `rotid` STRING,
  `mpx_chnl_id` SMALLINT,
  `url` STRING,
  `mppid` STRING,
  `page_name` STRING,
  `referer` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_traffic_source_1';

drop table if exists ubi_w.uts_v2_traffic_source_2;
CREATE TABLE ubi_w.uts_v2_traffic_source_2 (
  `global_session_id` STRING,
  `guid` STRING,
  `session_start_timestamp` BIGINT,
  `event_timestamp` BIGINT,
  `rotid` STRING,
  `mpx_chnl_id` SMALLINT,
  `url` STRING,
  `page_name` STRING,
  `referer` STRING,
  `chnl` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_traffic_source_2';

drop table if exists ubi_w.uts_v2_unified_traffic_source;
CREATE TABLE ubi_w.uts_v2_unified_traffic_source (
  `global_session_id` STRING,
  `guid` STRING,
  `traffic_source` STRING,
  `session_start_timestamp` BIGINT,
  `event_timestamp` BIGINT,
  `rotid` STRING,
  `mpx_chnl_id` SMALLINT,
  `url` STRING,
  `page_name` STRING,
  `referer` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_unified_traffic_source';

drop table if exists ubi_w.uts_v2_unified_traffic_source_backup;
CREATE TABLE ubi_w.uts_v2_unified_traffic_source_backup (
  `guid` STRING,
  `global_session_id` STRING,
  `abs_start_timestamp` BIGINT,
  `abs_end_timestamp` BIGINT,
  `session_start_dt` BIGINT,
  `traffic_source` MAP<STRING, STRING>,
  `bot_flag` STRUCT<`surface`: ARRAY<INT>, `ubi`: ARRAY<INT>, `utp`: ARRAY<INT>>,
  `others` MAP<STRING, STRING>,
  `bot_type` STRING,
  `dt` STRING,
  `data_type` STRING,
  `session_type` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
PARTITIONED BY (dt, data_type, session_type)
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_unified_traffic_source_backup';

drop table if exists `ubi_w`.`uts_unified_session_v1_no_trfc_src`;
CREATE TABLE `ubi_w`.`uts_unified_session_v1_no_trfc_src` (
  `guid` STRING,
  `global_session_id` STRING,
  `abs_start_timestamp` BIGINT,
  `abs_end_timestamp` BIGINT,
  `session_start_dt` BIGINT,
  `traffic_source` MAP<STRING, STRING>,
  `bot_flag` STRUCT<`surface`: ARRAY<INT>, `ubi`: ARRAY<INT>, `utp`: ARRAY<INT>>,
  `others` MAP<STRING, STRING>,
  `bot_type` STRING,
  `dt` STRING,
  `data_type` STRING,
  `session_type` STRING)
USING parquet
OPTIONS (
  `compression` 'snappy')
PARTITIONED BY (dt, data_type, session_type)
LOCATION 'viewfs://apollo-rno/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src';
