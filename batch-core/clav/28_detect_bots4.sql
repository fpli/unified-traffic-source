DROP TABLE IF EXISTS p_soj_cl_t.temp_csess25a_v9;
CREATE TABLE p_soj_cl_t.temp_csess25a_v9 USING parquet AS
SELECT
	s.guid,
	s.session_skey,
	s.site_id,
	s.session_start_dt,
	s.cobrand,
	s.session_cntry_id,
	s.start_timestamp
FROM
	p_soj_cl_t.temp_csess2a_v9 s
	INNER JOIN p_soj_cl_v.user_agents a
	ON (s.agent_id = a.agent_id)
WHERE
	s.cobrand NOT IN (2,3,4,5,9)
	AND s.valid_page_count = 1
	AND COALESCE(s.ref_domain, 'xxx') NOT LIKE ALL ('%.tiktok.%')
	AND COALESCE(sojlib.soj_nvl(SESSION_TRAFFIC_SOURCE_details, 'rd'),'xxx') NOT LIKE ALL ('%.tiktok.%')
	AND COALESCE(s.signedin_user_id,s.mapped_user_id) IS NULL
	AND a.agent_string NOT LIKE 'eBay%'
	AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '415' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE /* Newly created GUIDs. soj_guid_ts returns in UTC, while normal DW timestamps are always in MST, so there's a ~7-hour difference */ ;

SELECT 'Now waiting for surface_tracking.page_tracking_event_base_view ...';
DROP TABLE IF EXISTS p_soj_cl_t.temp_csess25b_v9;
CREATE TABLE p_soj_cl_t.temp_csess25b_v9 USING parquet AS
SELECT
	sess.guid,
	sess.session_skey,
	sess.site_id,
	sess.session_start_dt,
	sess.cobrand,
	sess.session_cntry_id,
	sess.start_timestamp,
	CASE WHEN sojlib.soj_nvl(sess.first_surf, 'ts') IS NULL OR sojlib.soj_nvl(sess.first_surf, 'ts') = '' THEN NULL ELSE CAST(sojlib.soj_nvl(sess.first_surf, 'ts') AS TIMESTAMP) END AS first_surf_ts,
	CAST(sojlib.soj_nvl(sess.first_surf, 'dwell') AS DECIMAL(24,6)) AS dwell_time_ms
FROM (
		SELECT
			sps.guid,
			sps.session_skey,
			sps.site_id,
			sps.session_start_dt,
			sps.cobrand,
			sps.session_cntry_id,
			sps.start_timestamp,
			MIN('ts=' || CAST(from_unixtime(surf.event_timestamp / 1000) AS STRING) || '&dwell=' || COALESCE(CAST(surf.dwell AS STRING), '')) AS first_surf
		FROM
			p_soj_cl_t.temp_csess25a_v9 sps
			LEFT JOIN surface_tracking.page_tracking_event_base_view surf
			ON (surf.guid = sps.guid AND surf.dt BETWEEN '2024-05-31' AND '2024-06-01' AND sps.start_timestamp BETWEEN from_unixtime(surf.event_timestamp /1000) - INTERVAL '5' MINUTE and from_unixtime(surf.event_timestamp /1000) + INTERVAL '30' MINUTE)
		GROUP BY 1,2,3,4,5,6,7
	) sess;

UPDATE t1
FROM
	p_soj_cl_t.temp_csess2a_v9 t1,
	p_soj_cl_t.temp_csess25b_v9 t2
SET
	bot_session = 1,
	bot_flags64 = t1.bot_flags64 + CASE WHEN CAST(t1.bot_flags64 AS BIGINT) & CAST(562949953421312 AS BIGINT) > 0 THEN 0 ELSE 562949953421312 END
WHERE
	t1.session_start_dt = t2.session_start_dt
	AND t1.guid = t2.guid
	AND t1.site_id = t2.site_id
	AND t1.cobrand = t2.cobrand
	AND t1.session_skey = t2.session_skey
	AND t2.first_surf_ts IS NULL
	AND t1.cobrand NOT IN (2,3,4,5,9)
	AND t1.valid_page_count = 1
	AND COALESCE(t1.signedin_user_id,t1.mapped_user_id) IS NULL
	AND sojlib.soj_guid_ts(t1.guid) BETWEEN t1.start_timestamp + INTERVAL '415' MINUTE AND t1.start_timestamp + INTERVAL '425' MINUTE /* Newly created GUIDs. soj_guid_ts returns in UTC, while normal DW timestamps are always in MST, so there's a ~7-hour difference */
	AND CAST(t1.bot_flags64 AS BIGINT) & CAST(562949953421312 AS BIGINT) = 0;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;
