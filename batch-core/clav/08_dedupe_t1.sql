DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1k_v9;
CREATE TABLE p_soj_cl_t.temp_csess1k_v9 USING delta AS
SELECT
	t1.guid,
	t1.session_skey,
	t1.site_id,
	t1.cobrand,
	t1.session_start_dt,
	CASE
		WHEN dupes > 1
			then array_sort(concat(min_page_list, max_page_list))
		else max_page_list
	END as page_list,
	t1.cguid,
	t1.valid_page_count,
	t1.min_sc_seqnum,
	t1.max_sc_seqnum,
	t1.user_id,
	t1.best_guess_user_id,
	REGEXP_REPLACE(t1.ip,'[\\[,\\]]','') AS ip,
	t1.agent_string,
	t1.gr_cnt,
	t1.gr_1_cnt,
	t1.vi_cnt,
	t1.homepage_cnt,
	t1.myebay_cnt,
	t1.signin_cnt,
	t1.siid_cnt,
	t1.nonjs_hp_cnt,
	t1.primary_app_id,
	t1.agent_id,
	t1.social_agent_type,
	t1.search_agent_type,
	t1.bot_flags64,
	t1.device_type,
	t1.agent_details,
	t1.session_flags64,
	t1.override_guid,
	t1.first_page,
	t1.last_page,
	t1.start_timestamp,
	t1.end_timestamp,
	CAST(sojlib.soj_nvl(t1.first_page, 'lp') AS INTEGER) AS lndg_page_id,
	CAST(sojlib.soj_nvl(t1.last_page, 'ep') AS INTEGER) AS exit_page_id,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'ref'), '%') AS STRING) AS lndg_referrer,
	CAST(sojlib.soj_nvl(t1.first_page, 'lnid') AS STRING) AS lndg_notification_id,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'url'), '%') AS STRING) AS lndg_page_url,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'lsid'), '%') AS STRING) AS lndg_sid,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'mppid'), '%') AS STRING) AS lndg_mppid,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'mnt'), '%') AS STRING) AS lndg_mnt,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'ort'), '%') AS STRING) AS lndg_ort,
	CAST(NULL AS INTEGER) AS ip_cntry_id
FROM (
		SELECT
			t.guid,
			t.session_skey,
			t.site_id,
			t.cobrand,
			MIN(t.session_start_dt) AS session_start_dt,
			MIN(t.page_list) as min_page_list,
			MAX(t.page_list) as max_page_list,
			MIN(t.cguid) AS cguid,
			SUM(t.valid_page_count) AS valid_page_count,
			MIN(t.min_sc_seqnum) AS min_sc_seqnum,
			MAX(t.max_sc_seqnum) AS max_sc_seqnum,
			MIN(t.user_id) AS user_id,
			MIN(t.best_guess_user_id) AS best_guess_user_id,
			CASE
				WHEN MAX(t.ip) LIKE '10.%'
					THEN MIN(t.ip)
				ELSE MAX(t.ip)
			END AS ip,
			MIN(t.agent_string) AS agent_string,
			SUM(t.gr_cnt) AS gr_cnt,
			SUM(t.gr_1_cnt) AS gr_1_cnt,
			SUM(t.vi_cnt) AS vi_cnt,
			SUM(t.homepage_cnt) AS homepage_cnt,
			SUM(t.myebay_cnt) AS myebay_cnt,
			SUM(t.signin_cnt) AS signin_cnt,
			SUM(t.siid_cnt) AS siid_cnt,
			SUM(t.nonjs_hp_cnt) AS nonjs_hp_cnt,
			MIN(t.primary_app_id) AS primary_app_id,
			MIN(t.agent_id) AS agent_id,
			MAX(t.social_agent_type) AS social_agent_type,
			MAX(t.search_agent_type) AS search_agent_type,
			MAX(t.bot_flags64) AS bot_flags64,
			MAX(t.device_type) AS device_type,
			MAX(t.agent_details) AS agent_details,
			MAX(t.session_flags64) AS session_flags64,
			MIN(t.override_guid) AS override_guid,
			MIN(t.first_page) AS first_page,
			MAX(t.last_page) AS last_page,
			MIN(t.start_timestamp) AS start_timestamp,
			MAX(t.end_timestamp) AS end_timestamp,
			COUNT(*) as dupes
		FROM
			p_soj_cl_t.temp_csess1j_v9 t
		GROUP BY 1,2,3,4
	) t1;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1l_v9;
CREATE TABLE p_soj_cl_t.temp_csess1l_v9 USING PARQUET AS
SELECT
	t1k.guid,
	t1k.session_skey,
	t1k.site_id,
	COUNT(*) AS numrecs
FROM
	p_soj_cl_t.temp_csess1k_v9 t1k
WHERE
	t1k.cobrand IN (0,6,7)
GROUP BY 1,2,3
HAVING numrecs > 1;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1m_v9;
CREATE TABLE p_soj_cl_t.temp_csess1m_v9 USING delta AS
SELECT
	t1.guid,
	t1.session_skey,
	t1.site_id,
	CASE
		WHEN t1.cobrand = 99
			THEN 6
		ELSE t1.cobrand
	END AS cobrand,
	t1.session_start_dt,
	t1.page_list,
	t1.cguid,
	t1.valid_page_count,
	t1.min_sc_seqnum,
	t1.max_sc_seqnum,
	t1.user_id,
	t1.best_guess_user_id,
	t1.ip,
	t1.agent_string,
	t1.gr_cnt,
	t1.gr_1_cnt,
	t1.vi_cnt,
	t1.homepage_cnt,
	t1.myebay_cnt,
	t1.signin_cnt,
	t1.siid_cnt,
	t1.nonjs_hp_cnt,
	t1.primary_app_id,
	t1.agent_id,
	t1.social_agent_type,
	t1.search_agent_type,
	t1.bot_flags64,
	t1.device_type,
	t1.agent_details,
	t1.session_flags64,
	t1.override_guid,
	t1.first_page,
	t1.last_page,
	t1.start_timestamp,
	t1.end_timestamp,
	CAST(sojlib.soj_nvl(t1.first_page, 'lp') AS INTEGER) AS lndg_page_id,
	CAST(sojlib.soj_nvl(t1.last_page, 'ep') AS INTEGER) AS exit_page_id,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'ref'), '%') AS STRING) AS lndg_referrer,
	CAST(sojlib.soj_nvl(t1.first_page, 'lnid') AS STRING) AS lndg_notification_id,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'url'), '%') AS STRING) AS lndg_page_url,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'lsid'), '%') AS STRING) AS lndg_sid,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'mppid'), '%') AS STRING) AS lndg_mppid,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'mnt'), '%') AS STRING) AS lndg_mnt,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(t1.first_page, 'ort'), '%') AS STRING) AS lndg_ort,
	CAST(NULL AS INTEGER) AS ip_cntry_id
FROM (
		SELECT
			t.guid,
			t.session_skey,
			t.site_id,
			MAX(
				CASE
					WHEN t.cobrand = 6
						THEN 99
					ELSE t.cobrand
				END) AS cobrand, /* Not sure if this is right, and we should always MAX */
			MIN(t.session_start_dt) AS session_start_dt,
			array_sort(concat(MIN(t.page_list), MAX(t.page_list))) page_list,
			MIN(t.cguid) AS cguid,
			SUM(t.valid_page_count) AS valid_page_count,
			MIN(t.min_sc_seqnum) AS min_sc_seqnum,
			MAX(t.max_sc_seqnum) AS max_sc_seqnum,
			MIN(t.user_id) AS user_id,
			MIN(t.best_guess_user_id) AS best_guess_user_id,
			CASE
				WHEN MAX(t.ip) LIKE '10.%'
					THEN MIN(t.ip)
				ELSE MAX(t.ip)
			END AS ip,
			MIN(t.agent_string) AS agent_string,
			SUM(t.gr_cnt) AS gr_cnt,
			SUM(t.gr_1_cnt) AS gr_1_cnt,
			SUM(t.vi_cnt) AS vi_cnt,
			SUM(t.homepage_cnt) AS homepage_cnt,
			SUM(t.myebay_cnt) AS myebay_cnt,
			SUM(t.signin_cnt) AS signin_cnt,
			SUM(t.siid_cnt) AS siid_cnt,
			SUM(t.nonjs_hp_cnt) AS nonjs_hp_cnt,
			MIN(t.primary_app_id) AS primary_app_id,
			MIN(t.agent_id) AS agent_id,
			MAX(t.social_agent_type) AS social_agent_type,
			MAX(t.search_agent_type) AS search_agent_type,
			MAX(t.bot_flags64) AS bot_flags64,
			MAX(t.device_type) AS device_type,
			MAX(t.agent_details) AS agent_details,
			MAX(t.session_flags64) AS session_flags64,
			MIN(t.override_guid) AS override_guid,
			MIN(t.first_page) AS first_page,
			MAX(t.last_page) AS last_page,
			MIN(t.start_timestamp) AS start_timestamp,
			MAX(t.end_timestamp) AS end_timestamp,
			count(*) AS numrows
		FROM
			p_soj_cl_t.temp_csess1k_v9 t
			INNER JOIN p_soj_cl_t.temp_csess1l_v9 dupe
			ON (t.guid = dupe.guid AND t.session_skey = dupe.session_skey AND t.site_id = dupe.site_id)
		WHERE
			t.cobrand IN (0,6,7)
		GROUP BY 1,2,3
	) t1;


DELETE t1
FROM
	p_soj_cl_t.temp_csess1k_v9 t1,
	p_soj_cl_t.temp_csess1l_v9 dup
WHERE
	t1.guid = dup.guid
	AND t1.session_skey = dup.session_skey
	AND t1.site_id = dup.site_id
	AND t1.cobrand IN (0,6,7);

INSERT INTO p_soj_cl_t.temp_csess1k_v9 SELECT * FROM p_soj_cl_t.temp_csess1m_v9;