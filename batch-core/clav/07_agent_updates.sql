DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1h_v9;
CREATE TABLE p_soj_cl_t.temp_csess1h_v9 USING PARQUET AS
SELECT
	tg1.agent_string,
	tg1.agent_id,
	COUNT(*) AS numrecs
FROM
	p_soj_cl_t.temp_csess1g_v9 tg1
WHERE
	agent_string IS NOT NULL
GROUP BY 1,2;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1i_v9;
CREATE TABLE p_soj_cl_t.temp_csess1i_v9 USING PARQUET AS
SELECT
	th1.agent_id,
	MAX(th1.agent_string) AS agent_string,
	MAX(th1.numrecs) AS numrecs,
	MAX(a.mobile_flag) AS mobile_flag,
	MAX(a.social_agent_type) AS social_agent_type,
	MAX(a.search_agent_type) AS search_agent_type,
	MAX(a.bot_ind) AS bot_ind,
	MAX(a.device_type) AS device_type,
	MAX(
		CASE WHEN a.browser_actual_type IS NULL THEN '' ELSE '&bt=' || CAST(a.browser_actual_type AS STRING) END
		|| CASE WHEN a.browser_actual_version IS NULL THEN '' ELSE '&bv=' || CAST(a.browser_actual_version AS STRING) END
		|| CASE WHEN a.hw_version IS NULL THEN '' ELSE '&hwv=' || CAST(a.hw_version AS STRING) END
		|| CASE WHEN a.os_type IS NULL THEN '' ELSE '&ost=' || CAST(a.os_type AS STRING) END
		|| CASE WHEN a.os_version IS NULL THEN '' ELSE '&osv=' || CAST(a.os_version AS STRING) END
	    ) AS agent_details
FROM
	p_soj_cl_t.temp_csess1h_v9 th1
	INNER JOIN p_soj_cl_t.user_agents a
	ON a.agent_id = th1.agent_id
GROUP BY 1;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1j_v9;
CREATE TABLE p_soj_cl_t.temp_csess1j_v9 USING PARQUET AS
SELECT
	tg1.guid,
	tg1.session_skey,
	tg1.site_id,
	CASE
		WHEN a.device_type = 'Tablet' AND a.mobile_flag = 1 AND tg1.cobrand = 0
			THEN 7
		WHEN a.mobile_flag = 1 AND tg1.cobrand = 0
			THEN 6
		WHEN a.mobile_flag = 1 AND tg1.cobrand = 1
			THEN 8
		WHEN a.mobile_flag = 1 AND tg1.cobrand = 5
			THEN 9
		ELSE tg1.cobrand
	END AS cobrand,
	tg1.session_start_dt,
	tg1.page_list,
	tg1.cguid,
	tg1.valid_page_count,
	tg1.min_sc_seqnum,
	tg1.max_sc_seqnum,
	tg1.user_id,
	tg1.best_guess_user_id,
	tg1.ip,
	tg1.agent_string,
	tg1.gr_cnt,
	tg1.gr_1_cnt,
	tg1.vi_cnt,
	tg1.homepage_cnt,
	tg1.myebay_cnt,
	tg1.signin_cnt,
	tg1.siid_cnt,
	tg1.nonjs_hp_cnt,
	CASE
		WHEN a.mobile_flag = 1 AND tg1.cobrand = 0 AND (a.device_type IS NULL OR a.device_type <> 'Tablet') AND tg1.site_id = 203
			THEN 4290
		WHEN a.mobile_flag = 1 AND tg1.cobrand = 0 AND (a.device_type IS NULL OR a.device_type <> 'Tablet')
			THEN 3564
		WHEN a.device_type = 'Tablet' AND tg1.cobrand = 6 AND tg1.primary_app_id = 1462
			THEN 2878
		ELSE primary_app_id
	END AS primary_app_id,
	tg1.agent_id,
	CASE
		WHEN a.bot_ind = 1
			THEN tg1.bot_flags64 + 2
		ELSE tg1.bot_flags64
	END AS bot_flags64,
	COALESCE(a.device_type, tg1.device_type) AS device_type,
	tg1.session_flags64,
	tg1.override_guid,
	tg1.user_name,
	tg1.lndg_page_id,
	tg1.start_timestamp,
	tg1.end_timestamp,
	tg1.exit_page_id,
	tg1.lndg_referrer,
	tg1.lndg_notification_id,
	tg1.lndg_page_url,
	tg1.lndg_sid,
	tg1.lndg_mppid,
	tg1.lndg_mnt,
	tg1.lndg_ort,
	a.social_agent_type,
	a.search_agent_type,
	a.agent_details,
	tg1.first_page,
	tg1.last_page
FROM
	p_soj_cl_t.temp_csess1g_v9 tg1
	LEFT OUTER JOIN p_soj_cl_t.temp_csess1i_v9 a
	ON a.agent_id = tg1.agent_id;