--------------------------------------------------------------------------------
-- Data quality check cookbook
-- https://zeta.dss.vip.ebay.com/zeta/share/#/notebook?notebookId=f34dfa18-8580-4ca9-9b17-9d99a93070c3
--------------------------------------------------------------------------------

-- %refresh_var(${UOW_FROM_DATE});
%refresh_var(${dt_1});
%refresh_var(${dt_1_formated});
%refresh_var(${dt_2});
%refresh_var(${dt_2_formated});

--------------------------------------------------------------------------------
-- ubi_w.uts_v2_unified_session_copy
--------------------------------------------------------------------------------

refresh table ubi_t.unified_session;
refresh table ubi_w.uts_v2_unified_session_copy;

select dt, data_type, session_type, count(1)
from ubi_w.uts_v2_unified_session_copy
where ((dt = '${dt_1}' and session_type = 'sameday')
  or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

select assert_true (a.cnt = b.cnt)
from
  (select count(DISTINCT guid, GLOBAL_SESSION_ID) cnt
   from ubi_t.unified_session
   where ((dt = '${dt_1}' and session_type = 'sameday')
     or (dt = '${dt_2}' and session_type = 'crossday'))) a
  join
  (select count(DISTINCT guid, GLOBAL_SESSION_ID) cnt
   from ubi_w.uts_v2_unified_session_copy
   where ((dt = '${dt_1}' and session_type = 'sameday')
     or (dt = '${dt_2}' and session_type = 'crossday'))) b;

--------------------------------------------------------------------------------
-- ubi_w.uts_v2_unified_traffic_source_backup
--------------------------------------------------------------------------------

refresh table ubi_w.uts_v2_unified_traffic_source_backup;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	495234
-- 20230318	patch	crossday	8580
-- 20230319	major	sameday	6081886395
-- 20230319	patch	sameday	971968
select dt, data_type, session_type, count(1)
from ubi_w.uts_v2_unified_traffic_source_backup
where ((dt = '${dt_1}' and session_type = 'sameday')
     or (dt = '${dt_2}' and session_type = 'crossday'))
group by dt, data_type, session_type;

-- dt	data_type	session_type	count(1)
-- 20230318	major	crossday	297661
-- 20230318	patch	crossday	8442
-- 20230319	major	sameday	122538894
-- 20230319	patch	sameday	66308
select dt, data_type, session_type, count(1)
from ubi_w.uts_v2_unified_traffic_source_backup
where ((dt = '${dt_1}' and session_type = 'sameday')
     or (dt = '${dt_2}' and session_type = 'crossday'))
  and traffic_source.traffic_source_level3 is not null
group by dt, data_type, session_type;

--------------------------------------------------------------------------------
-- ubi_t.unified_session
--------------------------------------------------------------------------------
refresh table ubi_t.unified_session;

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
-- compare with clav session
--------------------------------------------------------------------------------
refresh table ubi_t.unified_session;
select clav_traffic_source, unified_traffic_source, count(1)
from
	(select a.GUID, b.SOURCE_SESSION_SKEY, a.global_session_id, a.traffic_source.traffic_source_level3 as unified_traffic_source
	from ubi_t.unified_session a
	join ubi_t.unified_session_map b
	on a.guid = b.guid and a.global_session_id = b.global_session_id
	  and a.dt = '${dt_2}' and b.dt = '${dt_2}' and b.SOURCE = 'Ubi'
	  and a.traffic_source.traffic_source_level3 is not null) c
join
	(SELECT GUID, session_skey,
  		CASE
    		WHEN traffic_source_level3 LIKE 'Paid: Paid Search%' THEN 'Paid: Paid Search'
--     		WHEN traffic_source_level3 LIKE 'Organic: Txn Comms: Notifications%' OR traffic_source_level3 LIKE 'Free: Mktg Comms: Notifications%' THEN 'Notifications'
    		WHEN traffic_source_level3 LIKE 'Free: Free Social%' THEN 'Free: Free Social'
    		ELSE traffic_source_level3
  		END AS clav_traffic_source
	FROM access_views.clav_session_ext
	where session_start_dt = '${dt_2_formated}') d
on c.guid = d.guid and c.SOURCE_SESSION_SKEY = d.session_skey
group by 1, 2;

--------------------------------------------------------------------------------
-- ubi_w.uts_unified_session_v1_no_trfc_src
--------------------------------------------------------------------------------
ALTER TABLE ubi_w.uts_unified_session_v1_no_trfc_src RECOVER PARTITIONS;
show partitions ubi_w.uts_unified_session_v1_no_trfc_src;

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

--------------------------------------------------------------------------------
-- working table deep dive
--------------------------------------------------------------------------------

-- t-1=20230318: 310988666
-- t-1=20230319: 348274110
refresh table ubi_w.uts_v2_surface_event;
select count(1) from ubi_w.uts_v2_surface_event;
select * from ubi_w.uts_v2_surface_event limit 10;

-- t-1=20230318: 1162874963
-- t-1=20230319: 1285557945
refresh table ubi_w.uts_v2_ubi_event;
select count(1) from ubi_w.uts_v2_ubi_event;
select * from ubi_w.uts_v2_ubi_event limit 10;

-- t-1=20230318: 3205108
-- t-1=20230319: 3540247
refresh table ubi_w.uts_v2_deeplink_event;
select count(1) from ubi_w.uts_v2_deeplink_event;
select * from ubi_w.uts_v2_deeplink_event limit 10;

-- t-1=20230318: 116120985
-- source	count(1)
-- Ubi	107337241 / 116120985 = 92.4%
-- Surface	8783744 / 116120985 = 7.6%
-- t-1=20230319: 122897088
-- source	count(1)
-- Ubi	113456538 / 122897088 = 92.3%
-- Surface	9440550 / 122897088 = 7.7%
refresh table ubi_w.uts_v2_first_valid_event;
select count(1) from ubi_w.uts_v2_first_valid_event;
select source, count(1) from ubi_w.uts_v2_first_valid_event group by source;
select * from ubi_w.uts_v2_first_valid_event limit 10;

-- t-1=20230318: 22423513
-- t-1=20230319: 25070431
refresh table ubi_w.uts_v2_utp_event;
select count(1) from ubi_w.uts_v2_utp_event;
select * from ubi_w.uts_v2_utp_event limit 10;

-- t-1=20230318: 1018712
-- t-1=20230319: 1085948
refresh table ubi_w.uts_v2_imbd_event;
select count(1) from ubi_w.uts_v2_imbd_event;
select * from ubi_w.uts_v2_imbd_event limit 10;

-- t-1=20230318:
-- t-1=20230319: 122897088
refresh table ubi_w.uts_v2_unified_traffic_source;
select count(1) from ubi_w.uts_v2_unified_traffic_source;
select * from ubi_w.uts_v2_unified_traffic_source limit 10;
