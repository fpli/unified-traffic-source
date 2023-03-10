-- Prepare Surface events
-- Only Page events: Surface also tracks modules and clicks, but traffic source doesn't need those events
-- Only non-native events: Surface native data is from batchtrack, which is duplicate to UBI data, and Surface hasn't stored all payload tags
-- Valid page events: non-iframe pages
/*
drop table if exists ts_surface;
CREATE TABLE `default`.`ts_surface` (
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
USING parquet;
*/
insert into table ts_surface (
select d.global_session_id, d.source, d.source_session_skey, c.*
from
	(select GUID, SESSION_ID, a.PAGE_ID, b.PAGE_NAME, EVENT_TIMESTAMP, REFERER, PAGE_URL as URL
	from SURFACE_TRACKING.PAGE_TRACKING_EVENT_VIEW a
	inner join ACCESS_VIEWS.PAGES b
	on a.dt BETWEEN '2023-02-05' and '2023-02-06' and a.EXPERIENCE <> 'native' and b.IFRAME = 0 and a.PAGE_ID = b.PAGE_ID) c
inner join ubi_t.unified_session_map d
on d.dt = 20230205 and d.source = 'Surface' and c.guid = d.guid and c.session_id = d.source_session_skey
);

-- Prepare UBI events
-- Valid page events: non-iframe pages
/*
drop table if exists ts_ubi;
CREATE TABLE `default`.`ts_ubi` (
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
USING parquet;
*/
insert into table ts_ubi (
select d.global_session_id, d.source, d.source_session_skey, c.*
from
	(select GUID, SESSION_SKEY, a.PAGE_ID, b.PAGE_NAME, EVENT_TIMESTAMP, coalesce(REFERRER, soj_nvl(soj, 'ref')) as referer, URL_QUERY_STRING as URL
	from UBI_V.UBI_EVENT a
	inner join ACCESS_VIEWS.PAGES b
	on a.SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06' and b.IFRAME = 0 and a.PAGE_ID = b.PAGE_ID) c
inner join ubi_t.unified_session_map d
on d.dt = 20230205 and d.source = 'Ubi' and c.guid = d.guid and c.session_skey = d.source_session_skey
);

-- Prepare DeeplinkAction events
-- Referer fallback: Some native page events has no referer, but DeeplinkAction page records the real referer
drop table if exists ts_deeplink;
create table ts_deeplink as
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
		from UBI_V.UBI_EVENT
		where SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06'
			and PAGE_ID = 2367320
			and soj_nvl(soj, 'ref') is not null
			and soj_nvl(soj, 'ref') <> ''
			and soj_nvl(soj, 'ref') <> 'unknown') a
	inner join ubi_t.unified_session_map b
	on b.dt = 20230205 and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
where rk = 1;

-- Prepare first valid events
drop table if exists first_valid_event;
create table first_valid_event as
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
			(select guid, global_session_id, source, PAGE_ID, PAGE_NAME, CAST(EVENT_TIMESTAMP/1000 as bigint) as EVENT_TIMESTAMP, REFERER, URL from ts_surface
			union all
			select guid, global_session_id, source, PAGE_ID, PAGE_NAME, to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP, REFERER, URL from ts_ubi))
	where rk = 1) a
left join ts_deeplink b
on a.guid = b.guid and a.global_session_id = b.global_session_id;

-- Prepare UTP events
-- Including Chocolate Clicks and Push Notification Clicks
drop table if exists ts_utp;
create table ts_utp as
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
			from UBI_V.UBI_EVENT
			where SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06' and (PAGE_ID = 2547208 or (PAGE_ID = 2054060 and soj_nvl(soj, 'pnact') = '1'))) a
		inner join ubi_t.unified_session_map b
		on b.dt = 20230205 and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
	where rk = 1) c
left join CHOCO_DATA_V.DW_MPX_ROTATIONS d
on c.rotid = d.ROTATION_ID;

-- Prepare IMBD events
drop table if exists ts_imbd;
create table ts_imbd as
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
		from UBI_V.UBI_EVENT
		where SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06' and PAGE_ID = 2051248 and soj_nvl(soj, 'mppid') is not null and soj_nvl(soj, 'mppid') <> '') a
	inner join ubi_t.unified_session_map b
	on b.dt = 20230205 and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
where rk = 1;

-- 1. UTP traffic source
drop view if exists traffic_source_tmp;
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
FROM first_valid_event a
LEFT JOIN ts_utp b
ON
  -- 26, 27, 30 are Chocolate Message Center channels, they are on-ebay inbox mail traffic which will not be the traffic source
  b.chnl not in ('26', '27', '30')
  and a.global_session_id = b.global_session_id
  AND a.guid = b.guid
	AND ABS(a.event_timestamp - b.event_timestamp) <= 5;

-- 2. Chocolate traffic source fallback using first event's url
-- In some cases, the Chocolate Click is not in the same session with the first page event, but the first page event is triggered by Chocolate traffic
-- The first page URL will contain Chocolate parameters
drop view if exists traffic_source_tmp_1;
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

drop view if exists unified_ts_utp;
CREATE OR REPLACE TEMPORARY VIEW unified_ts_utp AS
SELECT
  *
FROM
  traffic_source_tmp_1
WHERE
  chnl IS NOT NULL;

drop view if exists unified_ts_unknown;
CREATE OR REPLACE TEMPORARY VIEW unified_ts_unknown AS
SELECT
  *
FROM
  traffic_source_tmp_1
WHERE
  chnl IS NULL;

-- 3. IMBD traffic source
drop view if exists unified_ts_imbd;
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
  LEFT JOIN ts_imbd b ON a.global_session_id = b.global_session_id AND a.guid = b.guid
  	AND ABS(a.session_start_timestamp - b.event_timestamp) <= 600;

drop table if exists unified_ts_1;
CREATE TABLE unified_ts_1 AS
SELECT
  *
FROM
  unified_ts_utp
UNION ALL
SELECT
  *
FROM
  unified_ts_imbd;

-- 4. Referer based traffic source
drop table if exists unified_ts_2;
CREATE TABLE unified_ts_2 AS
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
      referer regexp '%facebook%' OR referer LIKE '%twitter%' OR referer LIKE '%pinterest%' OR referer LIKE '%instagram%' OR referer LIKE '%linkedin%' OR referer LIKE '%t.co%'
    ) THEN 'Free: Free Social'
    WHEN referer LIKE '%mail%' THEN 'Organic: Txn Comms: Webmail w/o tracking'
    WHEN referer IS NULL OR referer = 'null' THEN 'Organic: Direct: No referer'
    WHEN referer IS NOT NULL THEN 'Free: Other'
    ELSE chnl
  END AS chnl
FROM
  unified_ts_1;

-- 5. Final result
drop table if exists unified_traffic_source;
CREATE TABLE unified_traffic_source AS
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
FROM unified_ts_2;



