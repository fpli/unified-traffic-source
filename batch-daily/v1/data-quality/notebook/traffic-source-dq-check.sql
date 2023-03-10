--------------------------------------------------------------------------------
-- Data quality check cookbook
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=4e065e50-5645-4b52-8e54-bfd162e22aa8
--------------------------------------------------------------------------------

-- %refresh_var(${UOW_FROM_DATE});
%refresh_var(${dt_1});
%refresh_var(${dt_1_formated});
%refresh_var(${dt_2});
%refresh_var(${dt_2_formated});

--------------------------------------------------------------------------------
-- ubi_w.tbd_ts_unified_session_snapshot
--------------------------------------------------------------------------------
refresh table ubi_w.tbd_ts_unified_session_snapshot;
show partitions ubi_w.tbd_ts_unified_session_snapshot;

-- dt	data_type	session_type	count(1)
-- 20230306	major	crossday	458013
-- 20230306	patch	crossday	4181
-- 20230307	major	sameday	6459790211
-- 20230307	patch	sameday	643658
-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	570259
-- 20230307	patch	crossday	4259
-- 20230308	major	sameday	6773903125
-- 20230308	patch	sameday	630428
select dt, data_type, session_type, count(1)
from ubi_w.tbd_ts_unified_session_snapshot
where ((dt = '${dt_1}' and session_type = 'sameday') 
  or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

select count(DISTINCT guid, GLOBAL_SESSION_ID) cnt
   from ubi_w.tbd_ts_unified_session_snapshot
   where ((dt = '${dt_1}' and session_type = 'sameday') 
     or (dt = '${dt_2}' and session_type = 'crossday'));

select assert_true (a.cnt = b.cnt)
from
  (select count(DISTINCT guid, GLOBAL_SESSION_ID) cnt
   from ubi_t.unified_session
   where ((dt = '${dt_1}' and session_type = 'sameday') 
     or (dt = '${dt_2}' and session_type = 'crossday'))) a
  join
  (select count(DISTINCT guid, GLOBAL_SESSION_ID) cnt
   from ubi_w.tbd_ts_unified_session_snapshot
   where ((dt = '${dt_1}' and session_type = 'sameday') 
     or (dt = '${dt_2}' and session_type = 'crossday'))) b;

--------------------------------------------------------------------------------
-- ubi_w.tbd_ts_unified_session_v1_no_trfc_src
--------------------------------------------------------------------------------

ALTER TABLE ubi_w.tbd_ts_unified_session_v1_no_trfc_src RECOVER PARTITIONS;
show partitions ubi_w.tbd_ts_unified_session_v1_no_trfc_src;

-- dt	data_type	session_type	count(1)
-- 20230306	major	crossday	458013
-- 20230306	patch	crossday	4181
-- 20230307	major	sameday	6459790211
-- 20230307	patch	sameday	643658
-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	570259
-- 20230307	patch	crossday	4259
-- 20230308	major	sameday	6773903125
-- 20230308	patch	sameday	630428
SELECT dt, data_type, session_type, count(1)
FROM ubi_w.tbd_ts_unified_session_v1_no_trfc_src a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday') 
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
group by dt, data_type, session_type;

-- empty
SELECT dt, data_type, session_type, count(1)
FROM ubi_w.tbd_ts_unified_session_v1_no_trfc_src a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday') 
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

--------------------------------------------------------------------------------
-- ubi_w.tbd_ts_unified_session_with_ts
--------------------------------------------------------------------------------

refresh table ubi_w.tbd_ts_unified_session_with_ts;
show partitions ubi_w.tbd_ts_unified_session_with_ts;

-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	570259
-- 20230307	patch	crossday	4259
-- 20230308	major	sameday	6773903125
-- 20230308	patch	sameday	630428
select dt, data_type, session_type, count(1)
from ubi_w.tbd_ts_unified_session_with_ts
where ((dt = '${dt_1}' and session_type = 'sameday') 
  or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	362296
-- 20230307	patch	crossday	4192
-- 20230308	major	sameday	128381047
-- 20230308	patch	sameday	68473
select dt, data_type, session_type, count(1)
from ubi_w.tbd_ts_unified_session_with_ts
where ((dt = '${dt_1}' and session_type = 'sameday') 
  or (dt = '${dt_2}' and session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

--------------------------------------------------------------------------------
-- ubi_t.unified_session
--------------------------------------------------------------------------------

-- dt	data_type	session_type	count(1)
-- 20230306	major	crossday	458013
-- 20230306	patch	crossday	4181
-- 20230307	major	sameday	6459790211
-- 20230307	patch	sameday	643658
-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	570259
-- 20230307	patch	crossday	4259
-- 20230308	major	sameday	6773903125
-- 20230308	patch	sameday	630428
refresh table ubi_t.unified_session;
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday') 
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
group by dt, data_type, session_type;


-- dt	data_type	session_type	count(1)
-- 20230306	major	crossday	379146
-- 20230306	patch	crossday	4140
-- 20230307	major	sameday	129802447
-- 20230307	patch	sameday	67110
-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	362296
-- 20230307	patch	crossday	4192
-- 20230308	major	sameday	128381047
-- 20230308	patch	sameday	68473
refresh table ubi_t.unified_session;
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday') 
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;


--------------------------------------------------------------------------------
-- ubi_t.unified_session_map
--------------------------------------------------------------------------------
-- dt	data_type	session_type	count(1)
-- 20230307	major	crossday	1640137
-- 20230307	patch	crossday	8532
-- 20230308	major	sameday	6872044758
-- 20230308	patch	sameday	631508
SELECT dt, data_type, session_type, count(1)
FROM ubi_t.unified_session_map a
WHERE ((a.dt = '${dt_1}' AND a.session_type = 'sameday') 
  or (a.dt = '${dt_2}' AND a.session_type = 'crossday'))
group by dt, data_type, session_type;


--------------------------------------------------------------------------------
-- compare with clav session
--------------------------------------------------------------------------------
select clav_traffic_source, unified_traffic_source, count(1)
from
	(select a.GUID, b.SOURCE_SESSION_SKEY, a.global_session_id, a.traffic_source.traffic_source_level3 as unified_traffic_source
	from ubi_t.unified_session a
	join ubi_t.unified_session_map b
	on a.guid = b.guid and a.global_session_id = b.global_session_id 
	  and a.dt = '${dt_2}' and b.dt = '${dt_2}'
	  and a.traffic_source.traffic_source_level3 is not null) c
join 
	(SELECT GUID, session_skey,
  		CASE
    		WHEN traffic_source_level3 LIKE 'Paid: Paid Search%' THEN 'Paid: Paid Search'
    		WHEN traffic_source_level3 LIKE 'Organic: Txn Comms: Notifications%' OR traffic_source_level3 LIKE 'Free: Mktg Comms: Notifications%' THEN 'Notifications'
    		WHEN traffic_source_level3 LIKE 'Free: Free Social%' THEN 'Free: Free Social'
    		ELSE traffic_source_level3
  		END AS clav_traffic_source
	FROM access_views.clav_session_ext
	where session_start_dt = '${dt_2_formated}') d
on c.guid = d.guid and c.SOURCE_SESSION_SKEY = d.session_skey
group by 1, 2;

