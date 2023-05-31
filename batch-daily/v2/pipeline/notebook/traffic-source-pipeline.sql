--------------------------------------------------------------------------------
-- Traffic source detection in unified session - batch pipeline v2
-- t handle t-1 sameday, t-2 crossday
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=52375d5e-8cd9-465b-9d8d-6566858cac8d
--
-- Based on unified-traffic-source/batch-core/v2 c089cceabf7ff4356806e37f9f0546cebeb6da83
-- https://github.corp.ebay.com/Unified-Tracking/unified-traffic-source/tree/main/batch-core/v2
--
-- Input tables:
-- ubi_t.unified_session_map
-- SURFACE_TRACKING.PAGE_TRACKING_EVENT_VIEW
-- UBI_V.UBI_EVENT
-- ACCESS_VIEWS.PAGES
-- CHOCO_DATA_V.DW_MPX_ROTATIONS
--
-- Target table:
-- ubi_t.unified_session
--
-- Working tables:
-- ubi_w.uts_v2_unified_session_copy
-- ubi_w.uts_v2_distilled_ubi_event
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
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Prepare dt variables
--------------------------------------------------------------------------------
-- e.g.
-- t = UOW_TO_DATE: 20230222
-- t-1 = UOW_FROM_DATE: 20230221
-- t-2: 20230220
--
-- Variables:
-- T, UOW_TO_DATE: 20230222
-- UOW_FROM_DATE: 20230221
-- dt_1: select '${UOW_FROM_DATE}': 20230221
-- dt_1_formated: select to_date('${UOW_FROM_DATE}', 'yyyyMMdd'): 2023-02-21
-- dt_2, select date_format(date_sub(to_date('${UOW_FROM_DATE}','yyyyMMdd'), 1), 'yyyyMMdd'): 20230220
-- dt_2_formated: select date_sub(to_date('${UOW_FROM_DATE}','yyyyMMdd'), 1): 2023-02-20

%refresh_var(${dt_1});
%refresh_var(${dt_1_formated});
%refresh_var(${dt_2});
%refresh_var(${dt_2_formated});

--------------------------------------------------------------------------------
-- [Placeholder] Watch done file
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Make a copy of unified session of t-1 sameday and t-2 crossday
--------------------------------------------------------------------------------
INSERT OVERWRITE TABLE ubi_w.uts_v2_unified_session_copy
SELECT *
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND (a.session_type = 'sameday' or a.session_type = 'open'))
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'));


--------------------------------------------------------------------------------
-- 1. Prepare unified session events for traffic source detection
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 1.1 Prepare distilled UBI events for performance tuning
--------------------------------------------------------------------------------

insert overwrite table ubi_w.uts_v2_distilled_ubi_event
select a.*
from UBI_V.UBI_EVENT a
inner join ACCESS_VIEWS.PAGES b
on a.PAGE_ID = b.PAGE_ID
where a.SESSION_START_DT BETWEEN '${dt_2_formated}' and '${dt_1_formated}'
and a.type != 'large'
and ((b.IFRAME = 0)
  or (a.PAGE_ID = 2367320)
  or (a.PAGE_ID = 2547208 or a.PAGE_ID = 2054060)
  or (a.PAGE_ID = 2051248));

--------------------------------------------------------------------------------
-- 1.2 Prepare valid events
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 1.2.1 Prepare Surface events
--------------------------------------------------------------------------------

-- Prepare Surface events
-- Only Page events: Surface also tracks modules and clicks, but traffic source doesn't need those events
-- Only non-native events: Surface native data is from batchtrack, which is duplicate to UBI data, and Surface hasn't stored all payload tags
-- Valid page events: non-iframe pages

-- Surface open event
CREATE OR REPLACE TEMPORARY VIEW surface_event_open
USING org.apache.spark.sql.parquet
OPTIONS (
  path "/apps/b_trk/export_tmp_dir/open_session_event/dt=${dt_1_formated}/*.parquet"
);

insert overwrite table ubi_w.uts_v2_surface_event
select d.global_session_id, d.source, d.source_session_skey, c.*
from
	((select GUID, SESSION_ID, a.PAGE_ID, b.PAGE_NAME, EVENT_TIMESTAMP, REFERER, PAGE_URL as URL
	from SURFACE_TRACKING.PAGE_TRACKING_EVENT_VIEW a
	inner join ACCESS_VIEWS.PAGES b
	on a.dt BETWEEN '${dt_2_formated}' and '${dt_1_formated}' and a.EXPERIENCE <> 'native' and b.IFRAME = 0 and a.PAGE_ID = b.PAGE_ID)
	union all
	(select GUID, sessionId as SESSION_ID, a.entityId as PAGE_ID, b.PAGE_NAME, eventTimestamp as EVENT_TIMESTAMP, REFERER, instanceId as URL
     from surface_event_open a
     inner join ACCESS_VIEWS.PAGES b on a.entityId = b.PAGE_ID
     where a.entityType = 'Page' and a.EXPERIENCE <> 'native' and b.IFRAME = 0)) c
inner join ubi_t.unified_session_map d
on ((d.dt = '${dt_1}' AND (d.session_type = 'sameday' or d.session_type = 'open')) or (d.dt = '${dt_2}' AND d.session_type = 'crossday'))
   and d.source = 'Surface' and c.guid = d.guid and c.session_id = d.source_session_skey;

--------------------------------------------------------------------------------
-- 1.2.2 Prepare UBI events
--------------------------------------------------------------------------------

-- Prepare UBI events
-- Valid page events: non-iframe pages
insert overwrite table ubi_w.uts_v2_ubi_event
select d.global_session_id, d.source, d.source_session_skey, c.*
from
	(select GUID, SESSION_SKEY, a.PAGE_ID, b.PAGE_NAME, EVENT_TIMESTAMP, coalesce(REFERRER, soj_nvl(soj, 'ref')) as referer, URL_QUERY_STRING as URL
	from ubi_w.uts_v2_distilled_ubi_event a
	inner join ACCESS_VIEWS.PAGES b
	on a.SESSION_START_DT BETWEEN '${dt_2_formated}' and '${dt_1_formated}' and (sojlib.soj_nvl(soj, 'rdt') is null or sojlib.soj_nvl(soj, 'rdt') = 0) and b.IFRAME = 0 and a.PAGE_ID = b.PAGE_ID) c
inner join ubi_t.unified_session_map d
on ((d.dt = '${dt_1}' AND (d.session_type = 'sameday' or d.session_type = 'open')) or (d.dt = '${dt_2}' AND d.session_type = 'crossday'))
   and d.source = 'Ubi' and c.guid = d.guid and c.session_skey = d.source_session_skey;

--------------------------------------------------------------------------------
-- 1.2.3 Prepare DeeplinkAction events
--------------------------------------------------------------------------------

-- Prepare DeeplinkAction events
-- Referer fallback: Some native page events has no referer, but DeeplinkAction page records the real referer
insert overwrite table ubi_w.uts_v2_deeplink_event
select *
from
	(select
		b.global_session_id,
		b.source_session_skey,
		a.*,
		ROW_NUMBER() OVER (PARTITION BY b.guid, global_session_id ORDER BY EVENT_TIMESTAMP ASC) AS rk
	from
		(select
			GUID,
			SESSION_SKEY,
			to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP,
			sojlib.soj_url_decode_escapes(soj_nvl(soj, 'ref'), '%') as referer
		from ubi_w.uts_v2_distilled_ubi_event
		where SESSION_START_DT BETWEEN '${dt_2_formated}' and '${dt_1_formated}'
			and PAGE_ID = 2367320
			and soj_nvl(soj, 'ref') is not null
			and soj_nvl(soj, 'ref') <> ''
			and soj_nvl(soj, 'ref') <> 'unknown') a
	inner join ubi_t.unified_session_map b
	on ((b.dt = '${dt_1}' AND (b.session_type = 'sameday' or b.session_type = 'open')) or (b.dt = '${dt_2}' AND b.session_type = 'crossday'))
	   and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
where rk = 1;

--------------------------------------------------------------------------------
-- 1.2.4 Prepare first valid events
--------------------------------------------------------------------------------

-- Prepare first valid events
insert overwrite table ubi_w.uts_v2_first_valid_event
select a.guid, a.global_session_id, a.source, a.PAGE_ID, a.PAGE_NAME, a.EVENT_TIMESTAMP, a.URL,
	case when a.REFERER is not null and a.REFERER <> 'null' then a.REFERER
		else b.REFERER
	end as REFERER,
	case when (a.REFERER is null or a.REFERER = 'null') and b.REFERER is not null then b.EVENT_TIMESTAMP
		else null
	end as deeplink_timestamp
from
	(select guid, global_session_id, source, PAGE_ID, PAGE_NAME, EVENT_TIMESTAMP, REFERER, URL
	from
		(select *, ROW_NUMBER() OVER (PARTITION BY guid, global_session_id ORDER BY EVENT_TIMESTAMP ASC) AS rk
		from
			(select guid, global_session_id, source, PAGE_ID, PAGE_NAME, CAST(EVENT_TIMESTAMP/1000 as bigint) as EVENT_TIMESTAMP, REFERER, URL from ubi_w.uts_v2_surface_event
			union all
			select guid, global_session_id, source, PAGE_ID, PAGE_NAME, to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP, REFERER, URL from ubi_w.uts_v2_ubi_event))
	where rk = 1) a
left join ubi_w.uts_v2_deeplink_event b
on a.guid = b.guid and a.global_session_id = b.global_session_id;

--------------------------------------------------------------------------------
-- 1.3 Prepare UTP events
--------------------------------------------------------------------------------

-- Prepare UTP events
-- Including Chocolate Clicks and Push Notification Clicks
refresh table ubi_w.uts_v2_utp_event;
insert overwrite table ubi_w.uts_v2_utp_event
select
	c.*,
	d.MPX_CHNL_ID
from
	(select *
	from
		(select
			b.global_session_id,
			b.source_session_skey,
			a.*,
			ROW_NUMBER() OVER (PARTITION BY b.guid, global_session_id ORDER BY EVENT_TIMESTAMP ASC) AS rk
		from
			(select
				GUID,
				SESSION_SKEY,
				to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP,
				case when PAGE_ID = 2547208 then soj_nvl(soj, 'chnl')
					when PAGE_ID = 2054060 then 'Notifications: Apps'
				end as chnl,
				cast(soj_nvl(soj, 'rotid') as bigint) as rotid,
				soj_url_decode_escapes(soj_nvl(soj, 'url_mpre'), '%') as url
			from ubi_w.uts_v2_distilled_ubi_event
			where SESSION_START_DT BETWEEN '${dt_2_formated}' and '${dt_1_formated}' and (PAGE_ID = 2547208 or (PAGE_ID = 2054060 and soj_nvl(soj, 'pnact') = '1'))) a
		inner join ubi_t.unified_session_map b
		on ((b.dt = '${dt_1}' AND (b.session_type = 'sameday' or b.session_type = 'open')) or (b.dt = '${dt_2}' AND b.session_type = 'crossday'))
		   and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
	where rk = 1) c
left join CHOCO_DATA_V.DW_MPX_ROTATIONS d
on c.rotid = d.ROTATION_ID;

--------------------------------------------------------------------------------
-- 1.4 Prepare IMBD events
--------------------------------------------------------------------------------

-- Prepare IMBD events
insert overwrite table ubi_w.uts_v2_imbd_event
select *
from
	(select
		b.global_session_id,
		b.source_session_skey,
		a.*,
		ROW_NUMBER() OVER (PARTITION BY b.guid, global_session_id ORDER BY EVENT_TIMESTAMP ASC) AS rk
	from
		(select
			GUID,
			SESSION_SKEY,
			to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP,
			'Organic: IMBD' as chnl,
			soj_nvl(soj, 'mppid') as mppid
		from ubi_w.uts_v2_distilled_ubi_event
		where SESSION_START_DT BETWEEN '${dt_2_formated}' and '${dt_1_formated}' and PAGE_ID = 2051248 and soj_nvl(soj, 'mppid') is not null and soj_nvl(soj, 'mppid') <> '') a
	inner join ubi_t.unified_session_map b
	on ((b.dt = '${dt_1}' AND (b.session_type = 'sameday' or b.session_type = 'open')) or (b.dt = '${dt_2}' AND b.session_type = 'crossday'))
	   and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
where rk = 1;

--------------------------------------------------------------------------------
-- 2. Detect traffic source based on first valid events, UTP and IMBD events
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 2.1 UTP traffic source
--------------------------------------------------------------------------------

-- 1. UTP traffic source
CREATE OR REPLACE TEMPORARY VIEW traffic_source_tmp AS
SELECT
	a.global_session_id,
	a.guid,
	a.event_timestamp as session_start_timestamp,
	b.event_timestamp,
	b.chnl,
	b.rotid,
	b.mpx_chnl_id,
	b.url,
	a.url as first_url,
	null as mppid,
	a.page_name,
	a.referer
FROM ubi_w.uts_v2_first_valid_event a
LEFT JOIN ubi_w.uts_v2_utp_event b
ON
  -- 26, 27, 30 are Chocolate Message Center channels, they are on-ebay inbox mail traffic which will not be the traffic source
  b.chnl not in ('26', '27', '30')
  and a.global_session_id = b.global_session_id
  AND a.guid = b.guid
	AND ABS(a.event_timestamp - b.event_timestamp) <= 5;


--------------------------------------------------------------------------------
-- 2.2 Chocolate traffic source fallback using first event's url
--------------------------------------------------------------------------------

-- 2. Chocolate traffic source fallback using first event's url
-- In some cases, the Chocolate Click is not in the same session with the first page event, but the first page event is triggered by Chocolate traffic
-- The first page URL will contain Chocolate parameters
CREATE OR REPLACE TEMPORARY VIEW traffic_source_tmp_1 AS
SELECT
	a.global_session_id,
	a.guid,
	a.session_start_timestamp,
	a.event_timestamp,
	case when a.chnl is not null then a.chnl
		when a.chnl_fallback is not null and a.chnl_fallback in (1, 2, 4, 7, 8, 16, 24, 28, 29) then a.chnl_fallback
		else null
	end as chnl,
	case when a.rotid is not null then a.rotid
		when b.mpx_chnl_id is not null then a.rotid_fallback
		else null
	end as rotid,
	case when a.mpx_chnl_id is not null then a.mpx_chnl_id
		else b.mpx_chnl_id
	end as mpx_chnl_id,
	a.url,
	a.mppid,
	a.page_name,
	a.referer
FROM
	(select
	parse_url(first_url, 'QUERY', 'mkcid') as chnl_fallback,
	parse_url(first_url, 'QUERY', 'mkrid') as rotid_fallback,
	*
	from traffic_source_tmp) a
left join CHOCO_DATA_V.DW_MPX_ROTATIONS b
on a.rotid_fallback = b.ROTATION_ID;

CREATE OR REPLACE TEMPORARY VIEW unified_ts_utp AS
SELECT
  *
FROM
  traffic_source_tmp_1
WHERE
  chnl IS NOT NULL;

CREATE OR REPLACE TEMPORARY VIEW unified_ts_unknown AS
SELECT
  *
FROM
  traffic_source_tmp_1
WHERE
  chnl IS NULL;

--------------------------------------------------------------------------------
-- 2.3 IMBD traffic source
--------------------------------------------------------------------------------

-- 3. IMBD traffic source
CREATE OR REPLACE TEMPORARY VIEW unified_ts_imbd AS
SELECT
  a.global_session_id,
  a.guid,
  a.session_start_timestamp,
  b.event_timestamp,
  b.chnl,
  NULL AS rotid,
  NULL AS mpx_chnl_id,
  NULL AS url,
  b.mppid,
  a.page_name,
  a.referer
FROM
  unified_ts_unknown a
  LEFT JOIN ubi_w.uts_v2_imbd_event b ON a.global_session_id = b.global_session_id AND a.guid = b.guid
  	AND ABS(a.session_start_timestamp - b.event_timestamp) <= 600;

insert overwrite TABLE ubi_w.uts_v2_traffic_source_1
SELECT
  *
FROM
  unified_ts_utp
UNION ALL
SELECT
  *
FROM
  unified_ts_imbd;

--------------------------------------------------------------------------------
-- 2.4 Referer based traffic source
--------------------------------------------------------------------------------

-- 4. Referer based traffic source
insert overwrite TABLE ubi_w.uts_v2_traffic_source_2
SELECT
  global_session_id,
  guid,
  session_start_timestamp,
  event_timestamp,
  rotid,
  mpx_chnl_id,
  url,
  page_name,
  referer,
  CASE
    WHEN chnl IS NOT NULL THEN chnl
    WHEN (
      referer LIKE '%google%' OR referer LIKE '%bing%' OR referer LIKE '%yahoo%' OR referer LIKE '%duckduckgo%' OR referer LIKE '%yandex%'
    )
    AND referer NOT LIKE '%mail%'
    AND page_name != 'Home Page' THEN 'Free: SEO: Natural Search'
    WHEN (
      referer LIKE '%google%' OR referer LIKE '%bing%' OR referer LIKE '%yahoo%' OR referer LIKE '%duckduckgo%' OR referer LIKE '%yandex%'
    )
    AND referer NOT LIKE '%mail%'
    AND page_name = 'Home Page' THEN 'Organic: Nav Search: Free'
    WHEN referer LIKE '%ebay%' THEN 'Organic: Direct: On eBay'
    WHEN (
      referer LIKE '%facebook%' OR referer LIKE '%twitter%' OR referer LIKE '%pinterest%' OR referer LIKE '%instagram%' OR referer LIKE '%linkedin%' OR referer LIKE '%t.co%'
    ) THEN 'Free: Free Social'
	  WHEN referer LIKE '%youtube%' THEN 'Free: Free Social'
    WHEN referer LIKE '%mail%' THEN 'Organic: Txn Comms: Webmail w/o tracking'
    WHEN referer IS NULL OR referer = 'null' THEN 'Organic: Direct: No referer'
    WHEN referer IS NOT NULL THEN 'Free: Other'
    ELSE chnl
  END AS chnl
FROM
  ubi_w.uts_v2_traffic_source_1;

--------------------------------------------------------------------------------
-- 2.5 Final result
--------------------------------------------------------------------------------

-- 5. Final result
insert overwrite TABLE ubi_w.uts_v2_unified_traffic_source
SELECT
  global_session_id,
  guid,
  CASE
    WHEN chnl = 'Organic: IMBD' OR mpx_chnl_id IN ('23', '15') THEN 'Organic: IMBD'
    WHEN (
      mpx_chnl_id = '2'
      AND url IS NOT NULL
      -- whitelist of common misspellings and ebay subsidiary
      AND lower(replace(sojlib.soj_url_decode_escapes(parse_url(url, 'QUERY', 'keyword'), '%'), ' ', '')) REGEXP '(e[a|+|-|.]?bay)|eaby|eby|eba|kijiji'
    ) OR (mpx_chnl_id = '25') THEN 'Organic: Nav Search: Paid'
	  WHEN mpx_chnl_id = '2' THEN 'Paid: Paid Search'
    WHEN mpx_chnl_id = '6' THEN 'Paid: ePN'
    WHEN mpx_chnl_id = '36' THEN 'Free: SEO: Free Feeds'
    WHEN mpx_chnl_id = '1' THEN 'Paid: Display'
    WHEN mpx_chnl_id IN ('33', '35') THEN 'Paid: Paid Social'
	  WHEN chnl = '16' THEN 'Free: Free Social'
    WHEN chnl = '7' THEN 'Organic: Txn Comms: Site Email'
    WHEN chnl = '24' THEN 'Free: Mktg Comms: SMS'
    WHEN chnl = '8' THEN 'Free: Mktg Comms: Mktg Email'
    WHEN chnl = '29' THEN 'Organic: Txn Comms: Customer Service Email'
    WHEN chnl = '1' THEN 'Paid: ePN'
    WHEN chnl = '28' THEN 'Free: SEO: Free Feeds'
    WHEN chnl = '4' THEN 'Paid: Display'
	  WHEN chnl = '2' THEN 'Paid: Paid Search'
    ELSE chnl
  END AS traffic_source,
  session_start_timestamp,
  event_timestamp,
  rotid,
  mpx_chnl_id,
  url,
  page_name,
  referer
FROM ubi_w.uts_v2_traffic_source_2;

--------------------------------------------------------------------------------
-- Merge traffic source with unified session into a swap table
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ubi_t.unified_session_swap
select
  b.guid,
  b.global_session_id,
  b.abs_start_timestamp,
  b.abs_end_timestamp,
  b.session_start_dt,
  map_filter(
    map('traffic_source_level3', c.traffic_source,
      'rotid', c.rotid,
	  'mpx_chnl_id', c.mpx_chnl_id,
	  'page_name', c.page_name,
	  'referer', sojlib.soj_get_url_domain(c.referer)),
	(k, v) -> v is not null and v <> 'null') traffic_source,
  b.bot_flag,
  b.others,
  b.bot_type,
  b.dt,
  b.data_type,
  b.session_type
from
  (select * from ubi_w.uts_v2_unified_session_copy a
  where ((a.dt = '${dt_1}' AND (a.session_type = 'sameday' or a.session_type = 'open'))
      or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))) b
  left join ubi_w.uts_v2_unified_traffic_source c
  on b.guid = c.guid and b.global_session_id = c.global_session_id;

--------------------------------------------------------------------------------
-- Backup before swap
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ubi_w.uts_v2_unified_traffic_source_backup
SELECT *
FROM ubi_t.unified_session_swap a
WHERE ((a.dt = '${dt_1}' AND (a.session_type = 'sameday' or a.session_type = 'open'))
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'));

--------------------------------------------------------------------------------
-- [Placeholder] move unified session with traffic source to target table
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Check before generate done file
--------------------------------------------------------------------------------

select assert_true(a.cnt = b.cnt)
from
  (select count(1) cnt
   from ubi_t.unified_session
   where ((dt = '${dt_1}' and (session_type = 'sameday' or session_type = 'open'))
     or (dt = '${dt_2}' and session_type = 'crossday'))) a
  join
  (select count(1) cnt
   from ubi_w.uts_v2_unified_session_copy
   where ((dt = '${dt_1}' and (session_type = 'sameday' or session_type = 'open'))
     or (dt = '${dt_2}' and session_type = 'crossday'))) b;

--------------------------------------------------------------------------------
-- [Placeholder] Generate done file
--------------------------------------------------------------------------------

