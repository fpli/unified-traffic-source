DELETE FROM p_soj_cl_t.agent_ip_list WHERE detect_date = '2024-06-01';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess7a_v9;
CREATE TABLE p_soj_cl_t.temp_csess7a_v9 USING PARQUET AS
SELECT
	COALESCE(s.agent_id, -1) AS agent_id,
	s.ip,
	COUNT(*) AS sessions,
	COUNT(DISTINCT s.guid) AS guids,
	COUNT(DISTINCT s.cguid) AS cguids,
	COUNT(DISTINCT s.signedin_user_id) AS uids,
	SUM(CASE WHEN s.cobrand IN (6) AND s.primary_app_id NOT IN (3564,1112) THEN 1 ELSE 0 END) AS mobile_sessions,
	SUM(CASE WHEN s.cguid IS NULL THEN 1 ELSE 0 END) AS nocguid_sessions,
	SUM(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END) AS sps_sessions,
	SUM(CASE WHEN s.valid_page_count = 2 THEN 1 ELSE 0 END) AS twopg_sessions,
	SUM(CASE WHEN s.valid_page_count = 3 THEN 1 ELSE 0 END) AS threepg_sessions,
	SUM(CASE WHEN s.valid_page_count = 4 THEN 1 ELSE 0 END) AS fourpg_sessions,
	SUM(CASE WHEN s.valid_page_count = 5 THEN 1 ELSE 0 END) AS fivepg_sessions,
	SUM(CASE WHEN s.valid_page_count > 1 THEN 1 ELSE 0 END) AS nonsps_sessions,
	SUM(CASE WHEN s.valid_page_count = s.homepage_cnt THEN 1 ELSE 0 END) AS homepg_sessions,
	SUM(CASE WHEN s.valid_page_count = s.vi_cnt THEN 1 ELSE 0 END) AS vi_sessions,
	SUM(CASE WHEN s.valid_page_count = s.signin_cnt THEN 1 ELSE 0 END) AS signin_sessions,
	SUM(CASE WHEN s.signedin_user_id IS NULL THEN 1 ELSE 0 END) AS nouid_sessions,
	SUM(CASE WHEN s.session_traffic_source_id = 1 THEN 1 ELSE 0 END) AS direct_sessions,
	SUM(CASE WHEN s.session_traffic_source_id IN (6,7,10,11,12,13,14,15,16,17,18,19) THEN 1 ELSE 0 END) AS mktg_sessions,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS newguid_sessions,
	MIN(s.valid_page_count) AS min_session_length,
	MAX(s.valid_page_count) AS max_session_length,
	CASE WHEN (sessions > 10 AND twopg_sessions = sessions) OR (sessions > 10 AND threepg_sessions = sessions) OR (sessions > 10 AND fourpg_sessions = sessions) OR (sessions > 10 AND fivepg_sessions = sessions) THEN 64 ELSE 0 END + CASE WHEN sessions > 20 AND sessions = mktg_sessions AND (guids = 1 OR guids = sessions) THEN 128 ELSE 0 END + CASE WHEN sessions > 3 AND homepg_sessions = sessions THEN 256 ELSE 0 END + CASE WHEN sessions > 5 AND vi_sessions = sessions THEN 512 ELSE 0 END + CASE WHEN sessions > 5 AND signin_sessions = sessions THEN 1024 ELSE 0 END AS bot_flags64
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.ip IS NOT NULL AND sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' /* Ensure we don't get screwed by evil GUIDs */
GROUP BY 1,2
HAVING
    (sessions > 10 AND twopg_sessions = sessions) /* Consistent two-page sessions */
    OR (sessions > 10 AND threepg_sessions = sessions) /* Consistent three-page sessions */
    OR (sessions > 10 AND fourpg_sessions = sessions) /* Consistent four-page sessions */
    OR (sessions > 10 AND fivepg_sessions = sessions) /* Consistent five-page sessions */
    OR (sessions > 20 AND sessions = mktg_sessions AND (guids = 1 OR guids = sessions)) /* Overly focused ON marketing - Likely click fraud */
    OR (sessions > 3 AND homepg_sessions = sessions) /* Catches a lot of random homepage-only visitors, AS well AS a ton of Safari Top Sites issues */
    OR (sessions > 5 AND vi_sessions = sessions) /* Lots of automated activity caught with this */
    OR (sessions > 5 AND signin_sessions = sessions) /* Lots of automated activity caught with this */;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess7b_v9;
CREATE TABLE p_soj_cl_t.temp_csess7b_v9 USING PARQUET AS
SELECT
	COALESCE(s.agent_id, -1) AS agent_id,
	s.ip,
	COUNT(*) AS sessions,
	COUNT(DISTINCT s.guid) AS guids,
	COUNT(DISTINCT s.cguid) AS cguids,
	COUNT(DISTINCT s.signedin_user_id) AS uids,
	SUM(CASE WHEN s.cobrand IN (6) AND s.primary_app_id NOT IN (3564,1112) THEN 1 ELSE 0 END) AS mobile_sessions,
	SUM(CASE WHEN s.cguid IS NULL THEN 1 ELSE 0 END) AS nocguid_sessions,
	SUM(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END) AS sps_sessions,
	SUM(CASE WHEN s.valid_page_count = 2 THEN 1 ELSE 0 END) AS twopg_sessions,
	SUM(CASE WHEN s.valid_page_count = 3 THEN 1 ELSE 0 END) AS threepg_sessions,
	SUM(CASE WHEN s.valid_page_count = 4 THEN 1 ELSE 0 END) AS fourpg_sessions,
	SUM(CASE WHEN s.valid_page_count = 5 THEN 1 ELSE 0 END) AS fivepg_sessions,
	SUM(CASE WHEN s.valid_page_count > 1 THEN 1 ELSE 0 END) AS nonsps_sessions,
	SUM(CASE WHEN s.valid_page_count = s.homepage_cnt THEN 1 ELSE 0 END) AS homepg_sessions,
	SUM(CASE WHEN s.valid_page_count = s.vi_cnt THEN 1 ELSE 0 END) AS vi_sessions,
	SUM(CASE WHEN s.valid_page_count = s.signin_cnt THEN 1 ELSE 0 END) AS signin_sessions,
	SUM(CASE WHEN s.signedin_user_id IS NULL THEN 1 ELSE 0 END) AS nouid_sessions,
	SUM(CASE WHEN s.session_traffic_source_id = 1 THEN 1 ELSE 0 END) AS direct_sessions,
	SUM(CASE WHEN s.session_traffic_source_id IN (6,7,10,11,12,13,14,15,16,17,18,19) THEN 1 ELSE 0 END) AS mktg_sessions,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS newguid_sessions,
	MIN(s.valid_page_count) AS min_session_length,
	MAX(s.valid_page_count) AS max_session_length,
	CASE WHEN sessions > 100 AND (vi_sessions >= 0.95 * sessions) THEN 32768 ELSE 0 END + CASE WHEN sessions > 100 AND (guids >= 0.98 * sessions) THEN 65536 ELSE 0 END + CASE WHEN sessions > 200 AND (cguids < 5) THEN 0 ELSE 0 END /* Deprecating this rule as CGUIDs are disappearing */ + CASE WHEN sessions > 1000 AND (max_session_length <= 10) THEN 262144 ELSE 0 END + CASE WHEN sessions > 10 AND (sessions = newguid_sessions) AND (cguids < 3 OR max_session_length < 10) THEN 0 ELSE 0 END + CASE WHEN sessions > 200 AND (newguid_sessions > 0.97 * sessions) THEN 1048576 ELSE 0 END AS bot_flags64
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.ip IS NOT NULL AND sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' /* Ensure we don't get screwed by evil GUIDs */
GROUP BY 1,2
HAVING
	(sessions > 50 AND (nouid_sessions = sessions OR nouid_sessions = 0) AND (mobile_sessions < (0.1 * sessions))) /* Not perfect yet, but pretty damn good */
	OR (sessions > 100 AND (vi_sessions >= 0.95 * sessions))
	OR (sessions > 100 AND (guids >= 0.98 * sessions))
	OR (sessions > 1000 AND (max_session_length <= 10))
	OR (sessions > 200 AND (newguid_sessions > 0.97 * sessions));

INSERT INTO p_soj_cl_t.agent_ip_list
(
	detect_date,
	agent_id,
	ip,
	sessions,
	guids,
	cguids,
	uids,
	mobile_sessions,
	nocguid_sessions,
	sps_sessions,
	twopg_sessions,
	threepg_sessions,
	fourpg_sessions,
	fivepg_sessions,
	nonsps_sessions,
	homepg_sessions,
	vi_sessions,
	signin_sessions,
	nouid_sessions,
	direct_sessions,
	mktg_sessions,
	newguid_sessions,
	min_session_length,
	max_session_length,
	bot_flags64
)
SELECT
	'2024-06-01' AS detect_date,
	COALESCE(t7a.agent_id, t7b.agent_id) AS agent_id,
	COALESCE(t7a.ip, t7b.ip) AS ip,
	COALESCE(t7a.sessions, t7b.sessions) AS sessions,
	COALESCE(t7a.guids, t7b.guids) AS guids,
	COALESCE(t7a.cguids, t7b.cguids) AS cguids,
	COALESCE(t7a.uids, t7b.uids) AS uids,
	COALESCE(t7a.mobile_sessions, t7b.mobile_sessions) AS mobile_sessions,
	COALESCE(t7a.nocguid_sessions, t7b.nocguid_sessions) AS nocguid_sessions,
	COALESCE(t7a.sps_sessions, t7b.sps_sessions) AS sps_sessions,
	COALESCE(t7a.twopg_sessions, t7b.twopg_sessions) AS twopg_sessions,
	COALESCE(t7a.threepg_sessions, t7b.threepg_sessions) AS threepg_sessions,
	COALESCE(t7a.fourpg_sessions, t7b.fourpg_sessions) AS fourpg_sessions,
	COALESCE(t7a.fivepg_sessions, t7b.fivepg_sessions) AS fivepg_sessions,
	COALESCE(t7a.nonsps_sessions, t7b.nonsps_sessions) AS nonsps_sessions,
	COALESCE(t7a.homepg_sessions, t7b.homepg_sessions) AS homepg_sessions,
	COALESCE(t7a.vi_sessions, t7b.vi_sessions) AS vi_sessions,
	COALESCE(t7a.signin_sessions, t7b.signin_sessions) AS signin_sessions,
	COALESCE(t7a.nouid_sessions, t7b.nouid_sessions) AS nouid_sessions,
	COALESCE(t7a.direct_sessions, t7b.direct_sessions) AS direct_sessions,
	COALESCE(t7a.mktg_sessions, t7b.mktg_sessions) AS mktg_sessions,
	COALESCE(t7a.newguid_sessions, t7b.newguid_sessions) AS newguid_sessions,
	COALESCE(t7a.min_session_length, t7b.min_session_length) AS min_session_length,
	COALESCE(t7a.max_session_length, t7b.max_session_length) AS max_session_length,
	COALESCE(t7a.bot_flags64,0) + COALESCE(t7b.bot_flags64,0) AS bot_flags64
FROM
	p_soj_cl_t.temp_csess7a_v9 t7a
	FULL OUTER JOIN p_soj_cl_t.temp_csess7b_v9 t7b
	ON (t7a.agent_id = t7b.agent_id AND t7a.ip = t7b.ip);

DELETE FROM p_soj_cl_t.ip_list WHERE detect_date = '2024-06-01';

INSERT INTO p_soj_cl_t.ip_list
(
	detect_date,
	ip,
	sessions,
	guids,
	mobile_sessions,
	nocguid_sessions,
	sps_sessions,
	twopg_sessions,
	threepg_sessions,
	fourpg_sessions,
	fivepg_sessions,
	nonsps_sessions,
	homepg_sessions,
	vi_sessions,
	signin_sessions,
	nouid_sessions,
	direct_sessions,
	mktg_sessions,
	newguid_sessions,
	agent_hopper,
	ip_hopper,
	min_session_length,
	max_session_length,
	bot_flags64
)
SELECT
	'2024-06-01' AS detect_date,
	s.ip,
	COUNT(*) AS sessions,
	COUNT(DISTINCT s.guid) AS guids,
	SUM(CASE WHEN s.cobrand IN (6) AND s.primary_app_id NOT IN (3564,1112) THEN 1 ELSE 0 END) AS mobile_sessions,
	SUM(CASE WHEN s.cguid IS NULL THEN 1 ELSE 0 END) AS nocguid_sessions,
	SUM(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END) AS sps_sessions,
	SUM(CASE WHEN s.valid_page_count = 2 THEN 1 ELSE 0 END) AS twopg_sessions,
	SUM(CASE WHEN s.valid_page_count = 3 THEN 1 ELSE 0 END) AS threepg_sessions,
	SUM(CASE WHEN s.valid_page_count = 4 THEN 1 ELSE 0 END) AS fourpg_sessions,
	SUM(CASE WHEN s.valid_page_count = 5 THEN 1 ELSE 0 END) AS fivepg_sessions,
	SUM(CASE WHEN s.valid_page_count > 1 THEN 1 ELSE 0 END) AS nonsps_sessions,
	SUM(CASE WHEN s.valid_page_count = s.homepage_cnt THEN 1 ELSE 0 END) AS homepg_sessions,
	SUM(CASE WHEN s.valid_page_count = s.vi_cnt THEN 1 ELSE 0 END) AS vi_sessions,
	SUM(CASE WHEN s.valid_page_count = s.signin_cnt THEN 1 ELSE 0 END) AS signin_sessions,
	SUM(CASE WHEN s.signedin_user_id IS NULL THEN 1 ELSE 0 END) AS nouid_sessions,
	SUM(CASE WHEN s.session_traffic_source_id = 1 THEN 1 ELSE 0 END) AS direct_sessions,
	SUM(CASE WHEN s.session_traffic_source_id IN (6,7,10,11,12,13,14,15,16,17,18,19) THEN 1 ELSE 0 END) AS mktg_sessions,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS newguid_sessions,
	SUM(CASE WHEN CAST(s.session_flags64 AS BIGINT) & CAST(4 AS BIGINT) > 0 THEN 1 ELSE 0 END) AS agent_hopper,
	SUM(CASE WHEN CAST(s.session_flags64 AS BIGINT) & CAST(2 AS BIGINT) > 0 THEN 1 ELSE 0 END) AS ip_hopper,
	MIN(s.valid_page_count) AS min_session_length,
	MAX(s.valid_page_count) AS max_session_length,
	0 AS bot_flags64
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.ip IS NOT NULL AND sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' /* Ensure we don't get screwed by evil GUIDs */
GROUP BY 1,2
HAVING ( (sessions >= 20 AND agent_hopper = sessions) /* Sessions with multiple agents in a session */ OR (sessions > 500) /* Just get any high-frequency sessions */ ) ;

DELETE FROM p_soj_cl_t.agent_list WHERE detect_date = '2024-06-01';
INSERT INTO p_soj_cl_t.agent_list
(
	detect_date,
	agent_id,
	sessions,
	guids,
	mobile_sessions,
	nocguid_sessions,
	sps_sessions,
	twopg_sessions,
	threepg_sessions,
	fourpg_sessions,
	fivepg_sessions,
	nonsps_sessions,
	homepg_sessions,
	vi_sessions,
	signin_sessions,
	nouid_sessions,
	direct_sessions,
	mktg_sessions,
	newguid_sessions,
	ip_hopper,
	min_session_length,
	unique_ips,
	max_session_length,
	bot_flags64
)
SELECT
	'2024-06-01' AS detect_date,
	s.agent_id,
	COUNT(*) AS sessions,
	COUNT(DISTINCT s.guid) AS guids,
	SUM(CASE WHEN s.cobrand IN (6) AND s.primary_app_id NOT IN (3564,1112) THEN 1 ELSE 0 END) AS mobile_sessions,
	SUM(CASE WHEN s.cguid IS NULL THEN 1 ELSE 0 END) AS nocguid_sessions,
	SUM(CASE WHEN s.valid_page_count = 1 THEN 1 ELSE 0 END) AS sps_sessions,
	SUM(CASE WHEN s.valid_page_count = 2 THEN 1 ELSE 0 END) AS twopg_sessions,
	SUM(CASE WHEN s.valid_page_count = 3 THEN 1 ELSE 0 END) AS threepg_sessions,
	SUM(CASE WHEN s.valid_page_count = 4 THEN 1 ELSE 0 END) AS fourpg_sessions,
	SUM(CASE WHEN s.valid_page_count = 5 THEN 1 ELSE 0 END) AS fivepg_sessions,
	SUM(CASE WHEN s.valid_page_count > 1 THEN 1 ELSE 0 END) AS nonsps_sessions,
	SUM(CASE WHEN s.valid_page_count = s.homepage_cnt THEN 1 ELSE 0 END) AS homepg_sessions,
	SUM(CASE WHEN s.valid_page_count = s.vi_cnt THEN 1 ELSE 0 END) AS vi_sessions,
	SUM(CASE WHEN s.valid_page_count = s.signin_cnt THEN 1 ELSE 0 END) AS signin_sessions,
	SUM(CASE WHEN s.signedin_user_id IS NULL THEN 1 ELSE 0 END) AS nouid_sessions,
	SUM(CASE WHEN s.session_traffic_source_id = 1 THEN 1 ELSE 0 END) AS direct_sessions,
	SUM(CASE WHEN s.session_traffic_source_id IN (6,7,10,11,12,13,14,15,16,17,18,19) THEN 1 ELSE 0 END) AS mktg_sessions,
	SUM(CASE WHEN sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' AND sojlib.soj_guid_ts(s.guid) BETWEEN s.start_timestamp + INTERVAL '417' MINUTE AND s.start_timestamp + INTERVAL '425' MINUTE THEN 1 ELSE 0 END) AS newguid_sessions,
	SUM(CASE WHEN CAST(s.session_flags64 AS BIGINT) & CAST(2 AS BIGINT) > 0 THEN 1 ELSE 0 END) AS ip_hopper,
	COUNT(DISTINCT s.ip) AS unique_ips,
	MIN(s.valid_page_count) AS min_session_length,
	MAX(s.valid_page_count) AS max_session_length,
	0 AS bot_flags64
FROM
	p_soj_cl_t.temp_csess2a_v9 s
WHERE
	s.agent_id IS NOT NULL AND sojlib.soj_replace_char(s.guid, '01234567890abcdefABCDEF', '') = '' /* Ensure we don't get screwed by evil GUIDs */
GROUP BY 1,2
HAVING
	sessions > 100 AND ( (nocguid_sessions > (sessions * 0.95)) OR (sps_sessions > (sessions * 0.97)) OR (nouid_sessions > (sessions * 0.93)) OR (direct_sessions > (sessions * 0.93)) OR (mktg_sessions > (sessions * 0.93)) OR (unique_ips < (sessions * 0.01)) )

INSERT INTO p_soj_cl_t.ip_dns_lkp
(
	ip_addr,
	bot_ind,
	cre_ts,
	last_criteria_met_dt
)
SELECT
	COALESCE(aip.ip, ipl.ip) AS ip_addr,
	0 AS bot_ind,
	CURRENT_TIMESTAMP AS cre_ts,
	'2024-06-01' AS last_criteria_met_dt
FROM (
		SELECT
			ail.ip,
			SUM(ail.sessions) AS aip_sessions
		FROM
			p_soj_cl_t.agent_ip_list ail
		WHERE
			ail.detect_date = '2024-06-01'
		GROUP BY 1
		HAVING aip_sessions > 1000
	) aip
	FULL JOIN
	(
		SELECT
			iplst.ip
		FROM
			p_soj_cl_t.ip_list iplst
		WHERE
			iplst.detect_date = '2024-06-01'
			AND iplst.sessions > 1000
		GROUP BY 1
	) ipl
	ON (aip.ip = ipl.ip)
	LEFT JOIN
	p_soj_cl_t.ip_dns_lkp current_lkp
	ON (COALESCE(aip.ip, ipl.ip) = current_lkp.ip_addr)
WHERE
	current_lkp.ip_addr IS NULL;

UPDATE ip
FROM
	p_soj_cl_t.ip_dns_lkp ip,
	(
		SELECT
			ail.ip,
			SUM(ail.sessions) AS aip_sessions
		FROM
			p_soj_cl_t.agent_ip_list ail
		WHERE
			ail.detect_date = '2024-06-01'
		GROUP BY 1
		HAVING aip_sessions > 1000
	) aip
SET
	ip.last_criteria_met_dt = '2024-06-01'
WHERE
	ip.ip_addr = aip.ip;

UPDATE ip FROM
	p_soj_cl_t.ip_dns_lkp ip,
	(
		SELECT
			iplst.ip
		FROM
			p_soj_cl_t.ip_list iplst
		WHERE
			iplst.detect_date = '2024-06-01' AND iplst.sessions > 1000
		GROUP BY 1
	) ipl
SET
	ip.last_criteria_met_dt = '2024-06-01'
WHERE
	ip.ip_addr = ipl.ip;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.ip_dns_lkp ip
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(32 AS BIGINT) > 0 THEN 0 ELSE 32 END
WHERE
	t2.ip = ip.ip_addr AND ip.bot_ind = 1;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.agent_ip_list aip
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(aip.bot_flags64 AS BIGINT) > 0 THEN 0 ELSE aip.bot_flags64 END
WHERE
	aip.detect_date = '2024-06-01' AND t2.ip = aip.ip AND COALESCE(t2.agent_id, -1) = aip.agent_id AND aip.bot_flags64 > 0;