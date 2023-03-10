-- Schema
-- naming convention: working tables are prefixed with ubi_w.tbd_ts_
-- tbd: tracking behavior data
-- ts: traffic source

-- prepare surface data
-- only page events, non-native events, non-iframe events
-- source: SURFACE_TRACKING.PAGE_TRACKING_EVENT_VIEW, ACCESS_VIEWS.PAGES, ubi_t.unified_session_map
-- logic:
-- - dt mapping: unified_session_map dt in '20230205' <=> PAGE_TRACKING_EVENT_VIEW dt in ('2023-02-05', '2023-02-06')
-- - ubi_t.unified_session_map has open partition, we'd better not handle open partition in the first place. Traffic source for open session is not accurate.
-- - T handle T-1 sameday and T-2 crossday

insert overwrite table ubi_w.tbd_ts_surface_event
select 
  d.global_session_id,
  d.source,
  d.source_session_skey,
  c.*
from
	(select 
	  GUID, 
	  SESSION_ID, 
	  a.PAGE_ID, 
	  b.PAGE_NAME, 
	  EVENT_TIMESTAMP, 
	  REFERER
	from SURFACE_TRACKING.PAGE_TRACKING_EVENT_VIEW a
	inner join ACCESS_VIEWS.PAGES b
	on a.dt BETWEEN '2023-02-05' and '2023-02-06' 
	and a.EXPERIENCE <> 'native' 
	and b.IFRAME = 0 
	and a.PAGE_ID = b.PAGE_ID) c
inner join ubi_t.unified_session_map d
on d.dt = '20230205'
and d.source = 'Surface' 
and c.guid = d.guid 
and c.session_id = d.source_session_skey;

-- prepare ubi data
-- source: UBI_V.UBI_EVENT, ACCESS_VIEWS.PAGES, ubi_t.unified_session_map
-- logic:
-- - dt mapping: ubi_t.unified_session_map in '20230205' <=> UBI_V.UBI_EVENT SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06'?

-- 1914 seconds, 1931 seconds
insert overwrite table ubi_w.tbd_ts_ubi_event
select 
  d.global_session_id, 
  d.source, 
  d.source_session_skey, 
  c.*
from
	(select 
	  GUID, 
	  SESSION_SKEY, 
	  a.PAGE_ID, 
	  b.PAGE_NAME, 
	  EVENT_TIMESTAMP, 
	  coalesce(REFERRER, soj_nvl(soj, 'ref')) as referer
	from UBI_V.UBI_EVENT a
	inner join ACCESS_VIEWS.PAGES b
	on a.SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06' 
	and b.IFRAME = 0 
	and a.PAGE_ID = b.PAGE_ID) c
inner join ubi_t.unified_session_map d
on d.dt = '20230205' 
and d.source = 'Ubi' 
and c.guid = d.guid 
and c.session_skey = d.source_session_skey;

-- prepare first valid event

insert overwrite table ubi_w.tbd_ts_first_valid_event
select 
  guid, 
  global_session_id, 
  source, 
  PAGE_ID, 
  PAGE_NAME, 
  EVENT_TIMESTAMP, 
  REFERER
from
	(select 
	  *, 
	  ROW_NUMBER() OVER (PARTITION BY guid, global_session_id ORDER BY EVENT_TIMESTAMP ASC) AS rk
	from
		(select 
		  guid, 
		  global_session_id, 
		  source, 
		  PAGE_ID, 
		  PAGE_NAME, 
		  CAST(EVENT_TIMESTAMP/1000 as bigint) as EVENT_TIMESTAMP, 
		  REFERER 
		  from ubi_w.tbd_ts_surface_event 
		union all 
		select 
		  guid, 
		  global_session_id, 
		  source, 
		  PAGE_ID, 
		  PAGE_NAME, 
		  to_unix_timestamp(EVENT_TIMESTAMP) as EVENT_TIMESTAMP, 
		  REFERER 
		  from ubi_w.tbd_ts_ubi_event))
where rk = 1;

-- prepare utp event
-- source: UBI_V.UBI_EVENT, ubi_t.unified_session_map, CHOCO_DATA_V.DW_MPX_ROTATIONS
-- logic:
-- - (PAGE_ID = 2547208 or (PAGE_ID = 2054060 and soj_nvl(soj, 'pnact') = '1'))
-- - return chnl, rotid, url, mpx_chnl_id
-- 2313 seconds
insert overwrite table ubi_w.tbd_ts_utp_event
select 
	c.*, 
	d.MPX_CHNL_ID
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
				when PAGE_ID = 2054060 then 'Notification' 
			end as chnl, 
			soj_nvl(soj, 'rotid') as rotid, 
			soj_url_decode_escapes(soj_nvl(soj, 'url_mpre'), '%') as url
		from UBI_V.UBI_EVENT 
		where SESSION_START_DT BETWEEN '2023-02-05' AND '2023-02-06' 
		and (PAGE_ID = 2547208 or (PAGE_ID = 2054060 and soj_nvl(soj, 'pnact') = '1'))) a
	inner join ubi_t.unified_session_map b
	on b.dt = '20230205' and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey) c
left join CHOCO_DATA_V.DW_MPX_ROTATIONS d
on c.rk = 1 and c.rotid = d.ROTATION_ID;

-- return first utp event in session
insert overwrite table ubi_w.tbd_ts_first_utp_event
select 
  global_session_id, 
  guid, 
  source_session_skey, 
  session_skey, 
  event_timestamp, 
  chnl, 
  rotid, 
  url, 
  mpx_chnl_id 
from ubi_w.tbd_ts_utp_event
where rk = 1;

-- prepare imbd event
-- source: UBI_V.UBI_EVENT, ubi_t.unified_session_map
-- logic:
-- - return the first imbd event
-- - return chnl, mppid
-- 2012 seconds
insert overwrite table ubi_w.tbd_ts_imbd_event
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
		where SESSION_START_DT BETWEEN '2023-02-04' AND '2023-02-06' 
		and PAGE_ID = 2051248 
		and soj_nvl(soj, 'mppid') is not null and soj_nvl(soj, 'mppid') <> '') a
	inner join ubi_t.unified_session_map b
	on b.dt = 20230205 and b.source = 'Ubi' and a.guid = b.guid and a.session_skey = b.source_session_skey)
where rk = 1;

-------------------------------------------------------------------
-- 1. UTP traffic source
-------------------------------------------------------------------
-- logic:
-- - ABS(a.event_timestamp - b.event_timestamp) <= 5

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
  null as mppid,
  a.page_name,
  a.referer
FROM
  ubi_w.tbd_ts_first_valid_event a
  LEFT JOIN ubi_w.tbd_ts_first_utp_event b 
  ON b.chnl not in ('26', '27', '30') 
  and a.global_session_id = b.global_session_id 
  AND a.guid = b.guid
  AND ABS(a.event_timestamp - b.event_timestamp) <= 5;
		
-------------------------------------------------------------------
-- 2. IMBD traffic source
-------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW unified_ts_utp AS
SELECT
  *
FROM
  traffic_source_tmp
WHERE
  chnl IS NOT NULL;

CREATE OR REPLACE TEMPORARY VIEW unified_ts_unknown AS
SELECT
  *
FROM
  traffic_source_tmp
WHERE
  chnl IS NULL;

-- logic:
-- - ABS(a.session_start_timestamp - b.event_timestamp) <= 600
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
  LEFT JOIN ubi_w.tbd_ts_imbd_event b 
  ON a.global_session_id = b.global_session_id AND a.guid = b.guid
  AND ABS(a.session_start_timestamp - b.event_timestamp) <= 600;
	

insert OVERWRITE TABLE ubi_w.tbd_ts_unified_ts_1
SELECT
  *
FROM
  unified_ts_utp
UNION ALL
SELECT
  *
FROM
  unified_ts_imbd;

-------------------------------------------------------------------
-- 3. referer based
-------------------------------------------------------------------
insert overwrite TABLE ubi_w.tbd_ts_unified_ts_2
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
  ubi_w.tbd_ts_unified_ts_1;

-------------------------------------------------------------------
-- 4. final result
-------------------------------------------------------------------
insert overwrite TABLE ubi_w.tbd_ts_unified_traffic_source
SELECT
  global_session_id,
  guid,
  CASE
    WHEN chnl = 'PN' THEN 'Notifications: Apps'
    WHEN chnl = 'Organic: IMBD' OR mpx_chnl_id IN ('23', '15') THEN 'Organic: IMBD'
    WHEN (
      mpx_chnl_id = '2'
      AND url IS NOT NULL
      AND parse_url(url, 'QUERY', 'keyword') LIKE '%ebay%'
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
FROM
  ubi_w.tbd_ts_unified_ts_2;

