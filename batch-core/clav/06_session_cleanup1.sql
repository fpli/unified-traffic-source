DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1e_v9;
CREATE TABLE p_soj_cl_t.temp_csess1e_v9 USING PARQUET AS
SELECT
	x.*,
	CAST(
		CASE
			WHEN CAST(CONV(SUBSTR(syslib.udf_sha256_latin(agent_string), 1, 16),16,10) AS DECIMAL(20,0)) > 9223372036854775808
				THEN CAST(CONV(SUBSTR(syslib.udf_sha256_latin(agent_string), 1, 16),16,10) AS DECIMAL(20,0)) - CAST(18446744073709551616 AS DECIMAL(20,0))
			ELSE CAST(CONV(SUBSTR(syslib.udf_sha256_latin(agent_string), 1, 16),16,10) AS DECIMAL(20,0))
		END AS BIGINT) AS agent_id
	FROM (
			SELECT
				guid,
				session_skey,
				site_id,
				cobrand,
				session_start_dt,
				page_list,
				CASE
					WHEN sojlib.is_decimal(sojlib.soj_nvl(first_uid, 'u'), 18, 0) = 1
						THEN CAST(sojlib.soj_nvl(first_uid, 'u') AS DECIMAL(18,0))
				END AS user_id,
				CASE
					WHEN sojlib.is_decimal(sojlib.soj_nvl(first_bguid, 'bu'), 18, 0) = 1
						THEN CAST(sojlib.soj_nvl(first_bguid, 'bu') AS DECIMAL(18,0))
				END AS best_guess_user_id,
				CAST(sojlib.soj_nvl(first_cguid, 'cguid') AS STRING) AS cguid,
				CASE
					WHEN sojlib.is_integer(sojlib.soj_nvl(first_appid, 'app')) = 1
						THEN CAST(sojlib.soj_nvl(first_appid, 'app') AS INTEGER)
					ELSE NULL
				END AS primary_app_id,
				CASE
					WHEN first_external_ip IS NOT NULL
						THEN CAST(sojlib.soj_nvl(first_external_ip, 'extip') AS STRING)
					WHEN first_external_ip2 IS NOT NULL
						THEN CAST(sojlib.soj_nvl(first_external_ip2, 'extip2') AS STRING)
					WHEN first_internal_ip IS NOT NULL
						THEN CAST(sojlib.soj_nvl(first_internal_ip, 'intip') AS STRING)
					ELSE NULL
				END AS ip,
				CAST(
					CASE
						WHEN sojlib.soj_nvl(first_page, 'agent') IS NOT NULL AND sojlib.soj_nvl(last_page, 'agent') IS NOT NULL AND sojlib.soj_nvl(first_page, 'agent') NOT LIKE ALL ('eBay%','ebay%') AND sojlib.soj_nvl(last_page, 'agent') NOT LIKE ALL ('eBay%','ebay%') AND coalesce(sojlib.soj_nvl(first_appid, 'app'),9999) not in (1462,2571,2878,35023,35024) AND sojlib.soj_nvl(first_page, 'agent') NOT LIKE ALL ('% OPR/%', 'GingerClient%') AND sojlib.soj_nvl(last_page, 'agent') NOT LIKE ALL ('% OPR/%', 'GingerClient%') AND sojlib.soj_nvl(first_page, 'agent') <> sojlib.soj_nvl(last_page, 'agent')
							THEN 549755813888
						ELSE 0
					END AS BIGINT) AS bot_flags64,
				CAST(
					CASE
						WHEN TRIM(sojlib.soj_replace_char(sojlib.soj_nvl(first_username, 'un'), '*', '')) = ''
							THEN NULL ELSE sojlib.soj_nvl(first_username, 'un')
					END AS STRING) AS user_name,
				CAST(sojlib.soj_nvl(first_page, 'lp') AS INTEGER) lndg_page_id,
				CAST(sojlib.soj_nvl(first_page, 'sqn') AS INTEGER) min_sc_seqnum,
				CAST(sojlib.soj_nvl(last_page, 'sqn') AS INTEGER) max_sc_seqnum,
				CAST(sojlib.soj_nvl(first_page, 'ts') AS STRING) start_timestamp,
				CAST(sojlib.soj_nvl(last_page, 'ts') AS STRING) end_timestamp,
				CAST(sojlib.soj_nvl(last_page, 'ep') AS INTEGER) exit_page_id,
				CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'ref'), '%') AS STRING) AS lndg_referrer,
				CAST(sojlib.soj_nvl(first_page, 'lnid') AS STRING) AS lndg_notification_id,
				CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'url'), '%') AS STRING) AS lndg_page_url,
				CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'lsid'), '%') AS STRING) AS lndg_sid,
				CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'mppid'), '%') AS STRING) AS lndg_mppid,
				CAST(LOWER(sojlib.soj_nvl(first_page, 'mnt')) AS STRING) AS lndg_mnt,
				CAST(sojlib.soj_nvl(first_page, 'ort') AS STRING) AS lndg_ort,
				CAST(
					CASE
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) LIKE 'ff%' AND sojlib.is_bigint(SUBSTR(COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string), 3, 999)) = 1
							THEN NULL
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE 'wpp[0-9]{1,20}'
							THEN NULL
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE '[0-9]{2}/[0-9]{2}/[0-9]{4} [012][0-9]:[0-5][0-9]:[0-5][0-9]'
							THEN NULL
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [012][0-9]:[0-5][0-9]:[0-5][0-9]'
							THEN NULL
						ELSE trim(COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string))
					END AS STRING) AS agent_string,
				CAST(
					CASE
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) LIKE 'ff%' AND sojlib.is_bigint(SUBSTR(COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string), 3, 999)) = 1
							THEN 2
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE 'wpp[0-9]{1,20}'
							THEN 2
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE '[0-9]{2}/[0-9]{2}/[0-9]{4} [012][0-9]:[0-5][0-9]:[0-5][0-9]'
							THEN 2
						WHEN COALESCE(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_page, 'agent'), '%'), sojlib.soj_url_decode_escapes(sojlib.soj_nvl(last_page, 'agent'), '%'), agent_string) RLIKE '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [012][0-9]:[0-5][0-9]:[0-5][0-9]'
							THEN 2
						ELSE 0
					END AS BIGINT) AS add_to_bot_flags64
			FROM
				p_soj_cl_t.temp_csess1d_v9
	    ) x;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1f_v9;
CREATE TABLE p_soj_cl_t.temp_csess1f_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	MAX(
		CASE
			WHEN sojlib.soj_nvl(e.soj, 'dsktop') = 'true'
				THEN 1
			WHEN sojlib.soj_nvl(e.soj, 'mobile') = 'true'
				THEN 2
			WHEN sojlib.soj_nvl(e.soj, 'tablet') = 'true'
				THEN 3
			ELSE NULL
		END) AS max_device_type
FROM
	ubi_v.ubi_event e
WHERE
	e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND e.page_id NOT IN (2616,2627,2835,3084,3085,3936,3962,3985,3994,4531,2046301,2053444,2054060,2054081,2054099,2054487,2056122,2056451,2057680,2060921,2061026,2067603,2140389,2154434,2208336,2259407,2309593,2317508,2321885,2356359,2367355,2368342,2370942,2376867,2379655,2380677,2388028,2402999,2403006,2492140,2500024,2500304,2502515,2507874,2515526,2530290,2542782,2544028,2545063,2552134)
	AND e.rdt = 0
GROUP BY 1,2,3
HAVING max_device_type IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1f2_v9;
CREATE TABLE p_soj_cl_t.temp_csess1f2_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	MAX(
		CASE
			WHEN sojlib.soj_nvl(e.soj, 'dsktop') = 'true'
				THEN 1
			WHEN sojlib.soj_nvl(e.soj, 'mobile') = 'true'
				THEN 2
			WHEN sojlib.soj_nvl(e.soj, 'tablet') = 'true'
				THEN 3
			ELSE NULL
		END) AS max_device_type
FROM
	p_soj_cl_t.temp_csess1bot_v9 e
WHERE
	e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND e.page_id NOT IN (2616,2627,2835,3084,3085,3936,3962,3985,3994,4531,2046301,2053444,2054060,2054081,2054099,2054487,2056122,2056451,2057680,2060921,2061026,2067603,2140389,2154434,2208336,2259407,2309593,2317508,2321885,2356359,2367355,2368342,2370942,2376867,2379655,2380677,2388028,2402999,2403006,2492140,2500024,2500304,2502515,2507874,2515526,2530290,2542782,2544028,2545063,2552134)
	AND e.rdt = 0
GROUP BY 1,2,3
HAVING max_device_type IS NOT NULL;

INSERT INTO p_soj_cl_t.temp_csess1f_v9 SELECT * FROM p_soj_cl_t.temp_csess1f2_v9;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1e2_v9;
CREATE TABLE p_soj_cl_t.temp_csess1e2_v9 USING PARQUET AS
SELECT
	t1e.guid,
	t1e.session_skey,
	t1e.session_start_dt,
	t1e.site_id,
	t1e.cobrand,
	u.newest_uid
FROM
	p_soj_cl_t.temp_csess1e_v9 t1e
	INNER JOIN (
		SELECT
			us.user_slctd_id,
			MAX(us.user_id) AS newest_uid
		FROM
			prs_secure_v.dw_users us
		GROUP BY 1
	) u
	ON (u.user_slctd_id = t1e.user_name);

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1g_v9;
CREATE TABLE p_soj_cl_t.temp_csess1g_v9 USING PARQUET AS
SELECT
	t1d.guid,
	t1d.session_skey,
	t1d.site_id,
	t1d.cobrand,
	t1d.session_start_dt,
	t1d.page_list,
	t1e.cguid,
	t1d.valid_page_count,
	t1e.min_sc_seqnum,
	t1e.max_sc_seqnum,
	coalesce(t1e.user_id, t1e2.newest_uid) AS user_id,
	t1e.best_guess_user_id,
	t1e.ip,
	t1e.agent_string,
	t1d.gr_cnt,
	t1d.gr_1_cnt,
	t1d.vi_cnt,
	t1d.homepage_cnt,
	t1d.myebay_cnt,
	t1d.signin_cnt,
	t1d.siid_cnt,
	t1d.nonjs_hp_cnt,
	t1e.primary_app_id,
	t1e.agent_id,
	t1e.bot_flags64 + t1e.add_to_bot_flags64 AS bot_flags64,
	CAST(
		CASE
			WHEN t1e.agent_string LIKE ANY ('eBay%','ebay%')
				THEN NULL
			WHEN t1e.primary_app_id NOT IN (1462, 2571, 2878, 35023, 35024)
				THEN NULL
			WHEN t1f.max_device_type = 3
				THEN 'Tablet'
			WHEN t1f.max_device_type = 2
				THEN 'Phone'
			WHEN t1f.max_device_type = 1
				THEN 'PC'
			ELSE NULL
		END AS STRING) AS device_type,
	t1d.session_flags64,
	t1d.override_guid,
	t1e.user_name,
	t1e.lndg_page_id,
	t1e.start_timestamp,
	t1e.end_timestamp,
	t1e.exit_page_id,
	t1e.lndg_referrer,
	t1e.lndg_notification_id,
	t1e.lndg_page_url,
	t1e.lndg_sid,
	t1e.lndg_mppid,
	t1e.lndg_mnt,
	t1e.lndg_ort,
	t1d.first_page,
	t1d.last_page
FROM
	p_soj_cl_t.temp_csess1d_v9 t1d
	INNER JOIN p_soj_cl_t.temp_csess1e_v9 t1e
	ON (t1e.guid = t1d.guid AND t1e.session_skey = t1d.session_skey AND t1e.site_id = t1d.site_id AND t1e.cobrand = t1d.cobrand AND t1e.session_start_dt = t1d.session_start_dt)
	LEFT OUTER JOIN p_soj_cl_t.temp_csess1f_v9 t1f
	ON (t1e.guid = t1f.guid AND t1e.session_skey = t1f.session_skey AND t1e.session_start_dt = t1f.session_start_dt)
	LEFT OUTER JOIN p_soj_cl_t.temp_csess1e2_v9 t1e2
	ON (t1e.guid = t1e2.guid AND t1e.session_skey = t1e2.session_skey AND t1e.site_id = t1e2.site_id AND t1e.cobrand = t1e2.cobrand AND t1e.session_start_dt = t1e2.session_start_dt);


