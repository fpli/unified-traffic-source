DELETE FROM p_soj_cl_t.rd_list WHERE detect_date = '2024-06-01';
INSERT INTO p_soj_cl_t.rd_list
(
	detect_date,
	ref_domain,
	sessions,
	nonsps,
	cguids,
	guids,
	rvs,
	ips,
	bot_flags64
)
SELECT
	'2024-06-01' AS detect_date,
	coalesce(s.ref_domain,sojlib.soj_nvl(s.SESSION_TRAFFIC_SOURCE_details,'rd')) as ref_domain2,
	COUNT(*) AS sessions,
	SUM(CASE WHEN s.valid_page_count > 1 THEN 1 ELSE 0 END) AS nonsps,
	COUNT(DISTINCT s.cguid) AS cguids,
	COUNT(DISTINCT s.guid) AS guids,
	COUNT(DISTINCT COALESCE(s.signedin_user_id, s.mapped_user_id)) AS rvs,
	COUNT(DISTINCT s.ip) AS ips,
	CASE WHEN ((rvs * 200) < sessions AND sessions > 1000 AND ref_domain2 NOT LIKE ALL ('%ebay.%', '%amazon.%', '%google.%', '%facebook%', '%twitter%', '%pinterest%', '%yahoo%', '%baidu%', '%instagram.%', '%reddit.%', '%gmail.%', '%tiktok.%')) THEN 33554432 ELSE 0 END + CASE WHEN (cguids * 100 < sessions AND sessions > 500 AND ref_domain2 NOT LIKE ALL ('%ebay.%', '%amazon.%', '%google.%', '%facebook%', '%twitter%', '%pinterest%', '%yahoo%', '%baidu%', '%instagram.%', '%reddit.%', '%gmail.%', '%tiktok.%')) THEN 0 ELSE 0 END + /* Deprecating this rule as CGUIDs are disappearing */ CASE WHEN (sessions > 1000 AND nonsps < 10 AND ref_domain2 NOT LIKE ALL ('%ebay.%', '%amazon.%', '%google.%', '%facebook%', '%twitter%', '%pinterest%', '%yahoo%', '%baidu%', '%instagram.%', '%reddit.%', '%gmail.%', '%tiktok.%')) THEN 134217728 ELSE 0 END AS bot_flags64
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	coalesce(s.ref_domain,sojlib.soj_nvl(s.SESSION_TRAFFIC_SOURCE_details,'rd')) IS NOT NULL
GROUP BY 1,2
HAVING
	((rvs * 200) < sessions AND sessions > 1000 AND ref_domain2 NOT LIKE ALL ('%ebay.%', '%amazon.%', '%google.%', '%facebook%', '%twitter%', '%pinterest%', '%yahoo%', '%baidu%', '%instagram.%', '%reddit.%', '%gmail.%', '%tiktok.%'))
	OR (sessions > 1000 AND nonsps < 10 AND ref_domain2 NOT LIKE ALL ('%ebay.%', '%amazon.%', '%google.%', '%facebook%', '%twitter%', '%pinterest%', '%yahoo%', '%baidu%', '%instagram.%', '%reddit.%', '%gmail.%', '%tiktok.%'));

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.rd_list rd
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(rd.bot_flags64 AS BIGINT) > 0 THEN 0 ELSE rd.bot_flags64 END
WHERE
	t2.ref_domain IS NOT NULL AND t2.ref_domain = rd.ref_domain AND rd.detect_date = '2024-06-01';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess7d_v9;
CREATE TABLE p_soj_cl_t.temp_csess7d_v9 USING PARQUET AS
SELECT
	t2.lndg_page_id,
	t2.exit_page_id,
	COUNT(*) AS sess,
	SUM(CASE WHEN t2.valid_page_count > 10 THEN 1 ELSE 0 END) AS vpc_gt_10,
	COUNT(DISTINCT COALESCE(t2.signedin_user_id, t2.mapped_user_id)) AS rvs,
	SUM(CASE WHEN sojlib.soj_replace_char(t2.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(t2.guid) BETWEEN t2.start_timestamp + INTERVAL '417' MINUTE AND t2.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	CASE WHEN sess > 1000 AND lndg_page_id = exit_page_id AND vpc_gt_10 < sess * 0.03 AND rvs < 0.1 * sess THEN 1 WHEN sess > 1000 AND vpc_gt_10 < sess * 0.03 AND rvs < 0.1 * sess THEN 2 WHEN sess > 1000 AND new_guid_sess > 0.97 * sess THEN 3 ELSE -999 END AS rule_matched
FROM
	p_soj_cl_t.temp_csess2a_v9 t2
WHERE
	t2.cobrand NOT IN (2,3,4,5,9) AND (t2.primary_app_id IS NULL OR t2.primary_app_id NOT IN (2571,1462,2878,35023,35024)) AND t2.site_id NOT IN (203) AND t2.valid_page_count > 1
GROUP BY 1,2
HAVING
	(sess > 1000 AND lndg_page_id = exit_page_id AND vpc_gt_10 < sess * 0.03 AND rvs < 0.1 * sess)
	OR (sess > 1000 AND vpc_gt_10 < sess * 0.03 AND rvs < 0.1 * sess)
	OR (sess > 1000 AND new_guid_sess > 0.97 * sess);

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.temp_csess7d_v9 t7d
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(536870912 AS BIGINT) > 0 THEN 0 ELSE 536870912 END
WHERE
	t2.lndg_page_id = t7d.lndg_page_id
	AND t2.exit_page_id = t7d.exit_page_id
	AND t2.cobrand NOT IN (2,3,4,5,9)
	AND t2.site_id NOT IN (203)
	AND t2.valid_page_count > 1
	AND ( t7d.rule_matched = 1 /* First rule can work across all non-app traffic */ OR (t7d.rule_matched > 1 AND CASE WHEN sojlib.soj_replace_char(t2.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(t2.guid) BETWEEN t2.start_timestamp + INTERVAL '417' MINUTE AND t2.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END = 1) /* Second and third rules should only run for new GUIDs */ );

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess7e_v9;
CREATE TABLE p_soj_cl_t.temp_csess7e_v9 USING PARQUET AS
SELECT
	t2.agent_id,
	t2.lndg_page_id,
	COUNT(*) AS sess,
	SUM(CASE WHEN t2.valid_page_count > 10 THEN 1 ELSE 0 END) AS vpc_gt_10,
	COUNT(DISTINCT COALESCE(t2.signedin_user_id, t2.mapped_user_id)) AS rvs
FROM
	p_soj_cl_t.temp_csess2a_v9 t2
WHERE
	t2.cobrand NOT IN (2,3,4,5,9) AND t2.site_id NOT IN (203) AND t2.agent_id IS NOT NULL
GROUP BY 1,2
HAVING
	(rvs < 10 AND sess > 1000 AND (vpc_gt_10 <= (sess * 0.01) OR vpc_gt_10 >= (sess * 0.98)))
	OR (sess > 1000 AND sess >= ( 1000 * rvs) AND (vpc_gt_10 <= (sess * 0.01) OR vpc_gt_10 >= (sess * 0.98)));

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.temp_csess7e_v9 t7e
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1073741824 AS BIGINT) > 0 THEN 0 ELSE 1073741824 END
WHERE
	t2.lndg_page_id = t7e.lndg_page_id AND t2.agent_id = t7e.agent_id AND t2.cobrand NOT IN (2,3,4,5,9) AND t2.site_id NOT IN (203);

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			s.guid
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
		WHERE CAST(CASE WHEN sojlib.is_bigint(sojlib.soj_nvl(lndg_page_url, '_pgn')) = 1 THEN sojlib.soj_nvl(lndg_page_url, '_pgn') ELSE NULL END AS BIGINT) > 30
		GROUP BY 1
	) lb
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8589934592 AS BIGINT) > 0 THEN 0 ELSE 8589934592 END
WHERE
	t2.guid = lb.guid;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			b.*
		FROM (
				SELECT
					s.guid,
					s.session_skey,
					s.cobrand,
					s.site_id,
					s.session_start_dt,
					s.valid_page_count,
					s.siid_cnt,
					CASE WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.end_timestamp, '\.')) = 0 THEN (CAST('000' AS decimal(8,3))/1000) WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.end_timestamp, '\.')) = 1 THEN (CAST(sojlib.soj_list_last_element(s.end_timestamp, '\.')||'00' AS decimal(8,3))/1000) WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.end_timestamp, '\.')) = 2 THEN (CAST(sojlib.soj_list_last_element(s.end_timestamp, '\.')||'0' AS decimal(8,3))/1000) ELSE (CAST(sojlib.soj_list_last_element(s.end_timestamp, '\.') AS decimal(8,3))/1000) END AS end_msec,
					CASE WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.start_timestamp, '\.')) = 0 THEN (CAST('000' AS decimal(8,3))/1000) WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.start_timestamp, '\.')) = 1 THEN (CAST(sojlib.soj_list_last_element(s.start_timestamp, '\.')||'00' AS decimal(8,3))/1000) WHEN CHAR_LENGTH(sojlib.soj_list_last_element(s.start_timestamp, '\.')) = 2 then (CAST(sojlib.soj_list_last_element(s.start_timestamp, '\.')||'0' AS decimal(8,3))/1000) ELSE (CAST(sojlib.soj_list_last_element(s.start_timestamp, '\.') AS decimal(8,3))/1000) END AS start_msec,
					(unix_timestamp(s.end_timestamp) + end_msec) - (unix_timestamp(s.start_timestamp) + start_msec) AS session_length_sec,
					CASE WHEN s.valid_page_count IS NULL OR s.valid_page_count = 0 THEN 0.0 ELSE CAST(session_length_sec AS DECIMAL(24,6)) / CAST(valid_page_count AS DECIMAL(24,6)) END AS ratio,
					COALESCE(s.signedin_user_id, s.mapped_user_id) AS uid
				FROM
					p_soj_cl_t.temp_csess2a_v9 s
				WHERE
					(s.primary_app_id IS NULL OR s.primary_app_id NOT IN (2878,1462,2571,35023,35024))
					AND s.cobrand IN (0,6,7)
					AND s.exit_page_id NOT IN (2057417) /* Weird exception for Hong Kong CBT dashboard, which has very weird tracking */
					AND s.start_timestamp IS NOT NULL
					AND s.end_timestamp IS NOT NULL
			) b
		WHERE CASE WHEN uid IS NULL AND valid_page_count BETWEEN 4 AND 10 AND ratio < 3.0 AND siid_cnt < 2 THEN 1 WHEN uid IS NULL AND valid_page_count BETWEEN 10 AND 20 AND ratio < 3.5 AND siid_cnt < 5 THEN 1 WHEN valid_page_count > 20 AND ratio < 6.0 AND valid_page_count > (3 * siid_cnt) THEN 1 ELSE 0 END = 1
	) tbots
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(17179869184 AS BIGINT) > 0 THEN 0 ELSE 17179869184 END
WHERE
	t2.guid = tbots.guid AND t2.session_skey = tbots.session_skey AND t2.site_id = tbots.site_id AND t2.cobrand = tbots.cobrand AND t2.session_start_dt = tbots.session_start_dt;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess7f_v9;
CREATE TABLE p_soj_cl_t.temp_csess7f_v9 USING PARQUET AS
SELECT
	s.session_start_dt,
	s.site_id,
	s.cobrand,
	CAST(TRIM(sojlib.soj_nvl(s.session_details, 'bt')) AS STRING) AS browser_actual_type1,
	CAST(TRIM(COALESCE(SUBSTRING(sojlib.soj_nvl(s.session_details, 'bv'),1,10), '0.0')) AS STRING) AS browser_actual_version1,
	SUM(CAST(1 AS BIGINT)) AS sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END AS BIGINT)) AS sps1,
	COUNT(DISTINCT s.guid) AS guids1,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 0 ELSE 1 END AS BIGINT)) AS reg_sess,
	SUM(CAST(CASE WHEN s.vi_cnt > 0 THEN 1 ELSE 0 END AS BIGINT)) AS vi_sess,
	SUM(CAST(CASE WHEN s.gr_cnt > 0 THEN 1 ELSE 0 END AS BIGINT)) AS gr_sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 2 THEN 1 ELSE 0 END AS BIGINT)) AS pg2_sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 3 THEN 1 ELSE 0 END AS BIGINT)) AS pg3_sess,
	SUM(CAST(CASE WHEN s.bot_session = 1 THEN 1 ELSE 0 END AS BIGINT)) AS bot_sess
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.cobrand NOT IN (2,3,4,5,9) AND sojlib.soj_nvl(s.session_details, 'bt') IS NOT NULL AND s.guid RLIKE '[a-z0-9]{32}'
GROUP BY 1,2,3,4,5
HAVING
	browser_actual_type1 NOT LIKE '% App' /* Exclude all the app-related agents */
	AND sess > 1000 /* Need sufficient sample size for the rules */
	AND site_id <> 100 /* Site 100 IS only really still used by sellers, I think, so should be excluded */
	AND ( (guids1 > 0.99 * sess) OR (sps1 > 0.98 * sess) OR ((sps1 + pg2_sess + pg3_sess) > 0.99 * sess) OR (new_guid_sess > 0.98 * sess) );

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			s.guid,
			s.session_skey,
			s.site_id,
			s.cobrand,
			s.session_start_dt,
			s.lndg_page_id
		FROM (
				SELECT
					t2a.guid,
					t2a.session_skey,
					t2a.site_id,
					t2a.cobrand,
					t2a.session_start_dt,
					CAST(TRIM(sojlib.soj_nvl(t2a.session_details, 'bt')) AS STRING) AS browser_actual_type,
					CAST(TRIM(COALESCE(SUBSTRING(sojlib.soj_nvl(t2a.session_details, 'bv'),1,10), '0.0')) AS STRING) AS browser_actual_version,
					CASE WHEN t2a.guid RLIKE '[a-z0-9]{32}' AND sojlib.soj_guid_ts(t2a.guid) BETWEEN t2a.start_timestamp + INTERVAL '417' MINUTE AND t2a.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END AS new_guid_ind,
					COALESCE(t2a.signedin_user_id, t2a.mapped_user_id) AS parent_uid,
					t2a.lndg_page_id,
					t2a.start_timestamp,
					t2a.end_timestamp
				FROM
					p_soj_cl_t.temp_csess2a_v9 t2a
				WHERE
					( t2a.primary_app_id IS NULL OR t2a.primary_app_id NOT IN (2571,1462,2878,35023,35024) )
					AND t2a.cobrand NOT IN (2,3,4,5,9)
					AND sojlib.soj_nvl(t2a.session_details, 'bt') IS NOT NULL
					AND t2a.guid RLIKE '[a-z0-9]{32}'
			) s
			INNER JOIN p_soj_cl_t.temp_csess7f_v9 t7f
			ON (t7f.site_id = s.site_id AND t7f.cobrand = s.cobrand AND t7f.session_start_dt = s.session_start_dt AND t7f.browser_actual_type1 = s.browser_actual_type AND t7f.browser_actual_version1 = s.browser_actual_version)
		WHERE
			s.new_guid_ind = 1 AND s.parent_uid IS NULL /* Don't update registered sessions */
	) b
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(34359738368 AS BIGINT) > 0 THEN 0 ELSE 34359738368 END
WHERE
	b.guid = t2.guid AND b.session_skey = t2.session_skey AND b.site_id = t2.site_id AND b.cobrand = t2.cobrand AND b.session_start_dt = t2.session_start_dt and b.lndg_page_id = t2.lndg_page_id;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.temp_csess16c_v9 tb
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE
											WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(137438953472 AS BIGINT) > 0
												THEN 0
											WHEN (tb.ak_bot1 > 0 OR tb.ak_bot2 > 0 OR tb.ak_bot3 > 0)
												THEN 137438953472
											ELSE 0
										END
									+ CASE
											WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(70368744177664 AS BIGINT) > 0
												THEN 0
											WHEN (tb.rw_bot > 0)
												THEN 70368744177664
											ELSE 0
										END
									+ CASE
											WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(274877906944 AS BIGINT) > 0
												THEN 0
											WHEN (tb.dst_bot > 0)
												THEN 274877906944
											ELSE 0
									    END
WHERE
	t2.guid = tb.guid AND t2.session_skey = tb.session_skey AND t2.session_start_dt = tb.session_start_dt;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			e.guid,
			e.session_skey,
			e.session_start_dt,
			e.site_id,
			MIN(e.seqnum) AS min_sc_seqnum,
			MAX(e.seqnum) AS max_sc_seqnum,
			COUNT(*) AS search_cnt
		FROM
			ubi_v.ubi_event e
		WHERE
			e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
			AND e.rdt = 0
			AND e.page_id IN (2045573, 2351460) /* Desktop Search pages */
			AND sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'sQr'), 18, 0) = 1 /* Item ID searches only */
		GROUP BY 1,2,3,4
	) sch
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1099511627776 AS BIGINT) > 0 THEN 0 ELSE 1099511627776 END
WHERE
	t2.guid = sch.guid AND t2.session_skey = sch.session_skey AND t2.site_id = sch.site_id AND t2.session_start_dt = sch.session_start_dt AND t2.valid_page_count = sch.search_cnt;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			e.guid,
			e.session_skey,
			e.session_start_dt,
			e.site_id,
			MIN(e.seqnum) AS min_sc_seqnum,
			MAX(e.seqnum) AS max_sc_seqnum,
			COUNT(*) AS search_cnt
		FROM
			p_soj_cl_t.temp_csess1bot_v9 e
		WHERE
			e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
			AND e.rdt = 0
			AND e.page_id IN (2045573, 2351460) /* Desktop Search pages */
			AND sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'sQr'), 18, 0) = 1 /* Item ID searches only */
		GROUP BY 1,2,3,4
	) sch
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1099511627776 AS BIGINT) > 0 THEN 0 ELSE 1099511627776 END
WHERE
	t2.guid = sch.guid AND t2.session_skey = sch.session_skey AND t2.site_id = sch.site_id AND t2.session_start_dt = sch.session_start_dt AND t2.valid_page_count = sch.search_cnt;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			s.lndg_page_url,
			SUM(CASE WHEN s.valid_page_count BETWEEN 2 AND 10 THEN 1 ELSE 0 END) AS lt10ps,
			SUM(CASE WHEN s.valid_page_count BETWEEN 2 AND 10 AND COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL AND sojlib.soj_guid_ts(s.guid) >= s.start_timestamp - INTERVAL '8' HOUR THEN 1 ELSE 0 END) AS lt10ps_newguid_unreg
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
		WHERE
			s.lndg_page_id NOT IN (2481888)
			AND s.cobrand IN (0,6,7)
			AND (s.primary_app_id IS NULL OR s.primary_app_id IN (3564))
			AND s.lndg_page_url IS NOT NULL
			AND ( s.lndg_page_url NOT LIKE '%apisd.ebay.%' AND s.lndg_page_url NOT LIKE '%/track%' AND s.lndg_page_url NOT LIKE '%.vip.%' AND s.lndg_page_url NOT LIKE '%.stratus.%' )
		GROUP BY 1
		HAVING
			lt10ps > 100 AND (lt10ps_newguid_unreg > (0.9 * lt10ps))
	) lp
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2199023255552 AS BIGINT) > 0 THEN 0 ELSE 2199023255552 END
WHERE
	t2.lndg_page_url = lp.lndg_page_url
	AND t2.lndg_page_url IS NOT NULL
	AND COALESCE(t2.signedin_user_id, t2.mapped_user_id) IS NULL
	AND sojlib.soj_guid_ts(t2.guid) >= t2.start_timestamp - INTERVAL '8' HOUR
	AND t2.cobrand IN (0,6,7)
	AND (t2.primary_app_id IS NULL OR t2.primary_app_id IN (3564))
	AND t2.lndg_page_url IS NOT NULL
	AND t2.valid_page_count BETWEEN 2 AND 10
	AND t2.lndg_page_id NOT IN (2481888);

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			s.agent_id,
			SUM(CASE WHEN s.gr_cnt > 0 THEN 1 ELSE 0 END) AS gr_sess,
			SUM(CASE WHEN s.vi_cnt > 0 THEN 1 ELSE 0 END) AS vi_sess,
			SUM(CASE WHEN s.gr_cnt > 0 or s.vi_cnt > 0 THEN 1 ELSE 0 END) AS vi_gr_sess,
			SUM(CASE WHEN s.bot_session > 0 THEN 1 ELSE 0 END) AS bot_sess,
			SUM(CASE WHEN s.session_traffic_source_id IN (1,2) THEN 1 ELSE 0 END) AS organic_sess,
			SUM(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END) AS sps_sess,
			SUM(CASE WHEN coalesce(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 1 ELSE 0 END) AS unreg_sess,
			SUM(CASE WHEN sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
			SUM(CASE WHEN s.bot_session = 0 THEN 1 ELSE 0 END) AS non_bot,
			SUM(CASE WHEN a.agent_string = lower(agent_string) THEN 1 ELSE 0 END) AS bad_agent_sess,
			COUNT(*) AS sessions
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
			INNER JOIN p_soj_cl_v.user_agents a
			ON a.agent_id = s.agent_id
		WHERE
			s.cobrand IN (0,6,7) AND coalesce(s.primary_app_id,3564) = 3564 AND a.agent_string NOT LIKE 'eBay%' /* avoid native accidentally tagged as mweb */
		GROUP BY 1
		HAVING
			(count(*) > 1000 AND new_guid_sess >= sessions * .98 AND unreg_sess >= sessions * .98 AND organic_sess >= sessions * .98)
			OR bad_agent_sess > 0
	) a
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(35184372088832 AS BIGINT) > 0 THEN 0 ELSE 35184372088832 END
WHERE
	t2.agent_id = a.agent_id AND t2.cobrand IN (0,6,7) AND (t2.primary_app_id IS NULL OR t2.primary_app_id IN (3564));

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess24a_v9;
CREATE TABLE p_soj_cl_t.temp_csess24a_v9 USING PARQUET AS
SELECT
	sojlib.soj_list_get_range_by_idx(ip, '.', 1, 2) AS ip_16_subnet,
	s.lndg_page_id,
	SUM(CAST(1 AS BIGINT)) AS sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END AS BIGINT)) AS sps,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 1 ELSE 0 END AS BIGINT)) AS unreg_sess
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.guid RLIKE '[a-z0-9]{32}' AND sojlib.soj_list_get_range_by_idx(ip, '.', 1, 2) IS NOT NULL AND s.lndg_page_id IS NOT NULL
GROUP BY 1,2
HAVING
	sess > 200 /* Need sufficient sample size for the rules */
	AND ( (unreg_sess > 0.99 * sess) OR (new_guid_sess > 0.98 * sess) );

UPDATE s
FROM
	p_soj_cl_t.temp_csess2a_v9 s,
	p_soj_cl_t.temp_csess24a_v9 ip16_lndg
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + CASE WHEN CAST(s.bot_flags64 AS BIGINT) & CAST(281474976710656 AS BIGINT) > 0 THEN 0 ELSE 281474976710656 END
WHERE
	s.lndg_page_id = ip16_lndg.lndg_page_id AND sojlib.soj_list_get_range_by_idx(s.ip, '.', 1, 2) = ip16_lndg.ip_16_subnet AND s.lndg_page_id IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess24b_v9;
CREATE TABLE p_soj_cl_t.temp_csess24b_v9 USING PARQUET AS
SELECT
	sojlib.soj_list_get_range_by_idx(ip, '.', 1, 2) AS ip_16_subnet,
	s.agent_id,
	SUM(CAST(1 AS BIGINT)) AS sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END AS BIGINT)) AS sps,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 1 ELSE 0 END AS BIGINT)) AS unreg_sess
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.guid RLIKE '[a-z0-9]{32}' AND sojlib.soj_list_get_range_by_idx(ip, '.', 1, 2) IS NOT NULL AND s.agent_id IS NOT NULL
GROUP BY 1,2
HAVING
	sess > 200 /* Need sufficient sample size for the rules */
	AND ( (unreg_sess > 0.99 * sess) OR (new_guid_sess > 0.98 * sess) );

UPDATE s
FROM
	p_soj_cl_t.temp_csess2a_v9 s,
	p_soj_cl_t.temp_csess24b_v9 ip16_agent
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + CASE WHEN CAST(s.bot_flags64 AS BIGINT) & CAST(281474976710656 AS BIGINT) > 0 THEN 0 ELSE 281474976710656 END
WHERE
	s.agent_id = ip16_agent.agent_id AND sojlib.soj_list_get_range_by_idx(s.ip, '.', 1, 2) = ip16_agent.ip_16_subnet AND s.agent_id IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess24c_v9;
CREATE TABLE p_soj_cl_t.temp_csess24c_v9 USING PARQUET AS
SELECT
	sojlib.soj_list_get_range_by_idx(ip, '.', 1, 3) AS ip_24_subnet,
	s.lndg_page_id,
	SUM(CAST(1 AS BIGINT)) AS sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END AS BIGINT)) AS sps,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 1 ELSE 0 END AS BIGINT)) AS unreg_sess
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.guid RLIKE '[a-z0-9]{32}' AND sojlib.soj_list_get_range_by_idx(ip, '.', 1, 3) IS NOT NULL AND s.lndg_page_id IS NOT NULL
GROUP BY 1,2
HAVING
	sess > 300 /* Need sufficient sample size for the rules */
	AND ( (unreg_sess > 0.99 * sess) OR (new_guid_sess > 0.98 * sess) );

UPDATE s
FROM
	p_soj_cl_t.temp_csess2a_v9 s,
	p_soj_cl_t.temp_csess24c_v9 ip24_lndg
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + CASE WHEN CAST(s.bot_flags64 AS BIGINT) & CAST(281474976710656 AS BIGINT) > 0 THEN 0 ELSE 281474976710656 END
WHERE
	s.lndg_page_id = ip24_lndg.lndg_page_id AND sojlib.soj_list_get_range_by_idx(s.ip, '.', 1, 3) = ip24_lndg.ip_24_subnet AND s.lndg_page_id IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess24d_v9;
CREATE TABLE p_soj_cl_t.temp_csess24d_v9 USING PARQUET AS
SELECT
	sojlib.soj_list_get_range_by_idx(ip, '.', 1, 3) AS ip_24_subnet,
	s.agent_id,
	SUM(CAST(1 AS BIGINT)) AS sess,
	SUM(CAST(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END AS BIGINT)) AS sps,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS new_guid_sess,
	SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 1 ELSE 0 END AS BIGINT)) AS unreg_sess
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.guid RLIKE '[a-z0-9]{32}' AND sojlib.soj_list_get_range_by_idx(ip, '.', 1, 3) IS NOT NULL AND s.agent_id IS NOT NULL
GROUP BY 1,2
HAVING
	sess > 300 /* Need sufficient sample size for the rules */
	AND ( (unreg_sess > 0.99 * sess) OR (new_guid_sess > 0.98 * sess) );

UPDATE s
FROM
	p_soj_cl_t.temp_csess2a_v9 s,
	p_soj_cl_t.temp_csess24d_v9 ip24_agent
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + CASE WHEN CAST(s.bot_flags64 AS BIGINT) & CAST(281474976710656 AS BIGINT) > 0 THEN 0 ELSE 281474976710656 END
WHERE
	s.agent_id = ip24_agent.agent_id AND sojlib.soj_list_get_range_by_idx(s.ip, '.', 1, 3) = ip24_agent.ip_24_subnet AND s.agent_id IS NOT NULL;

UPDATE
	p_soj_cl_t.temp_csess2a_v9 s
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + CASE WHEN CAST(s.bot_flags64 AS BIGINT) & CAST(68719476736 AS BIGINT) > 0 THEN 0 ELSE 68719476736 END
WHERE
	s.primary_app_id IN (1462,2571,2878) AND sojlib.soj_nvl(s.session_details, 'dn') LIKE ANY ('%x86%', '%x64%');

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			referrer,
			SUM(CASE WHEN valid_page_count = 1 then 1 else 0 end) AS sps_sessions,
			count(*) as ttl_session,
			SUM(CASE WHEN bot_session = 1 then 1 else 0 end) as current_bot_sessions,
			SUM(CASE WHEN bot_session = 0 then 1 else 0 end) as new_bot_sessions,
			SUM(CASE WHEN lndg_page_fmly4 in ('VI','GR') then 1 else 0 end) as vigr_sessions,
			SUM(CAST(CASE WHEN COALESCE(s.signedin_user_id, s.mapped_user_id) IS NULL THEN 0 ELSE 1 END AS BIGINT)) AS reg_sess
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
		WHERE
			referrer like any ('%/itm/%','findItemsByKeywords')
		group by 1
		having
			count(*) > 400
			and sps_sessions >= ttl_session*0.98
			and current_bot_sessions >= ttl_session*.5
			and vigr_sessions >= ttl_session*0.98
			and reg_sess <= ttl_session*0.02
	) ref
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(17592186044416 AS BIGINT) > 0 THEN 0 ELSE 17592186044416 END
WHERE
	t2.referrer = ref.referrer AND t2.cobrand IN (0,6,7) AND (t2.primary_app_id IS NULL OR t2.primary_app_id IN (3564));

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;