DROP TABLE IF EXISTS p_soj_cl_t.temp_csess2a1_v9;
CREATE TABLE p_soj_cl_t.temp_csess2a1_v9 USING delta AS
SELECT
	t1.guid,
	t1.session_skey,
	t1.site_id,
	t1.cobrand,
	t1.session_start_dt,
	t1.user_id,
	t1.best_guess_user_id,
	/**to make sure to format correctly IPV6 addresses**/
	CASE
		WHEN LENGTH(REGEXP_REPLACE(t1.ip, '[0-9:a-zA-Z]', '')) = 0 AND t1.ip LIKE '%::%'
			THEN REGEXP_REPLACE(t1.ip,'::',CASE WHEN t1.ip RLIKE '.+::$' THEN ARRAY_JOIN(ARRAY_REPEAT(':0',8 - SIZE(SPLIT(t1.ip,':')) + 2),'') ELSE ARRAY_JOIN(CONCAT(ARRAY(':'),ARRAY_REPEAT('0:',8 - SIZE(SPLIT(t1.ip,':')) + 1)),'') END)
		ELSE t1.ip
	END AS ip,
	CAST(NULL AS DECIMAL(18,0)) AS mapped_user_id,
	CAST(NULL AS DECIMAL(4,0)) AS session_cntry_id,
	CAST(NULL AS STRING) AS session_rev_rollup,
	CAST(NULL AS INTEGER) AS upd_type
FROM
	p_soj_cl_t.temp_csess1k_v9 t1;

SELECT 'Now waiting for ubi_guid_x_uid...';

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a1_v9 t2a,
	p_soj_cl_t.session_guid_uid_map b
SET
	t2a.mapped_user_id = b.mapped_uid
WHERE
	t2a.guid = b.guid
	AND t2a.session_start_dt = b.session_start_dt
	AND t2a.user_id IS NULL
	AND t2a.best_guess_user_id IS NULL
	AND b.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'；

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a1_v9 t2a,
	(
		SELECT
			t1.guid,
			t1.session_skey,
			t1.session_start_dt,
			t1.site_id,
			t1.cobrand,
			t1.user_id,
			t1.best_guess_user_id,
			t1.mapped_user_id,
			CAST(uch.user_cntry_id as DECIMAL(4,0)) AS user_cntry_id
		from
			p_soj_cl_t.temp_csess2a1_v9 t1
			INNER JOIN access_views.dw_user_cntry_hist uch
			ON (COALESCE(t1.user_id, t1.best_guess_user_id, t1.mapped_user_id) = uch.user_id AND t1.session_start_dt >= uch.start_dt AND t1.session_start_dt < uch.end_dt)
	) b
SET
	t2a.session_cntry_id = b.user_cntry_id
WHERE
	t2a.guid = b.guid
	AND t2a.session_skey = b.session_skey
	AND t2a.session_start_dt = b.session_start_dt
	AND t2a.site_id = b.site_id
	AND t2a.cobrand = b.cobrand;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess9b_v9;
CREATE TABLE p_soj_cl_t.temp_csess9b_v9 USING PARQUET AS
SELECT
	ip,
	CAST(SPLIT(ip,'\\.')[0] * POWER(256,3) + SPLIT(ip,'\\.')[1] * POWER(256,2) + SPLIT(ip,'\\.')[2] * POWER(256,1) + SPLIT(ip,'\\.')[3] * POWER(256,0) AS BIGINT) AS ip_long,
	session_start_dt,
	COUNT(*) numrows
FROM
	p_soj_cl_t.temp_csess2a1_v9 t1
WHERE
	ip IS NOT NULL
	AND sojlib.is_validIPv4(CAST(ip AS STRING)) = 1
	AND session_cntry_id is NULL
GROUP BY 1,2,3；

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess9d_v9;
CREATE TABLE p_soj_cl_t.temp_csess9d_v9 USING PARQUET AS
SELECT
	t1n.ip,
	t1n.session_start_dt,
	t1n.ip_long,
	ip2.ip_cntry_id,
	ip2.end_dt,
	COUNT(t1n.ip) AS cnt
FROM
	p_soj_cl_t.temp_csess9b_v9 t1n
	INNER JOIN p_soj_cl_t.zip_dim_explode ip2
	ON t1n.ip_long = ip2.ip_long
GROUP BY 1,2,3,4,5；

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a1_v9 t2a,
	p_soj_cl_t.temp_csess9d_v9 ip
SET
	t2a.session_cntry_id = ip.ip_cntry_id
WHERE
	t2a.ip = ip.ip
	and t2a.session_start_dt = ip.session_start_dt
	and t2a.session_cntry_id IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess9e_v9;
CREATE TABLE p_soj_cl_t.temp_csess9e_v9 USING PARQUET AS
SELECT
	ip ,
	LPAD(CONV(ARRAY_JOIN(TRANSFORM(SPLIT(ip,':'), x -> LPAD(x, 4, '0')), ''), 16, 10), 39, '0') AS ip_dec,
	COUNT(*) AS numrows
FROM
	p_soj_cl_t.temp_csess2a1_v9
WHERE
	ip IS NOT NULL
	AND session_cntry_id IS NULL
	AND sojlib.is_validIPv4(CAST(ip AS STRING)) = 0 /* non IPV4, although there might be some mixes IPV4/IPV6 but we just ignore them*/
	AND LENGTH(REGEXP_REPLACE(ip, '[0-9:a-zA-Z]', '')) = 0 /* IPV6 */
	AND SIZE(SPLIT(ip, ':')) = 8 /* IPV6 */
GROUP BY 1,2；

DROP TABLE IF EXISTS p_soj_cl_t.dw_zip_ipv6_dim;
CREATE TABLE p_soj_cl_t.dw_zip_ipv6_dim USING PARQUET AS
SELECT
	ip_begin_addr_txt,
	LPAD(CONV(ARRAY_JOIN(TRANSFORM(SPLIT(ip_begin_addr_txt,':'),x -> LPAD(x,4,'0')),''),16,10),39,'0') AS ip_start,
	ip_end_ip_addr_txt,
	LPAD(CONV(ARRAY_JOIN(TRANSFORM(SPLIT(ip_end_ip_addr_txt,':'),x -> LPAD(x,4,'0')),''),16,10),39,'0') AS ip_end,
	two_char_cntry_cd,
	city_name
FROM
	access_views.dw_zip_ipv6_dim;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess9f_v9;
CREATE TABLE p_soj_cl_t.temp_csess9f_v9 USING PARQUET OPTIONS (compression 'snappy') AS
SELECT
	t1n.ip,
	ip2.two_char_cntry_cd,
	CASE
		WHEN ip2.two_char_cntry_cd = 'uk'
			THEN 3
		WHEN ip2.two_char_cntry_cd = 'je'
			THEN 105
		WHEN ip2.two_char_cntry_cd = 'gg'
			THEN 86
		WHEN ip2.two_char_cntry_cd = 'cw'
			THEN 147
		WHEN ip2.two_char_cntry_cd = 'sx'
			THEN 147
		ELSE c.cntry_id
	END AS ip_cntry_id
FROM
	p_soj_cl_t.temp_csess9e_v9 t1n
	INNER JOIN p_soj_cl_t.dw_zip_ipv6_dim ip2
	ON t1n.ip_dec BETWEEN ip2.ip_start AND ip2.ip_end
	LEFT JOIN access_views.dw_countries c
	ON ip2.two_char_cntry_cd = c.iso_cntry_code AND (c.cntry_code NOT IN ('na') OR c.cntry_id = 143) ;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a1_v9 t2a,
	p_soj_cl_t.temp_csess9f_v9 t9f
SET
	t2a.session_cntry_id = t9f.ip_cntry_id,
	t2a.upd_type = 4
WHERE
	t2a.ip = t9f.ip
	and t2a.session_cntry_id IS NULL;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a1_v9 t2a,
	(
		SELECT
			cntry_id,
			rev_rollup
		FROM
			access_views.dw_countries
	) cntry
SET
	t2a.session_rev_rollup = CASE
	                            WHEN t2a.session_cntry_id IS NULL OR t2a.session_cntry_id = -999
	                                THEN 'UNKN'
	                            ELSE cntry.rev_rollup
	                         END
WHERE
	t2a.session_cntry_id = cntry.cntry_id AND t2a.session_rev_rollup IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess2a_v9;
CREATE TABLE p_soj_cl_t.temp_csess2a_v9 USING delta AS
SELECT
	t1.guid,
	t1.session_skey,
	t1.site_id,
	t1.cobrand,
	t1.session_start_dt,
	/*t1.page_list,*/
	t1.cguid,
	t1.valid_page_count,
	t1.min_sc_seqnum,
	t1.max_sc_seqnum,
	t2a.user_id AS signedin_user_id,
	coalesce(t2a.best_guess_user_id, t2a.mapped_user_id) as mapped_user_id,
	t1.agent_id,
	t1.social_agent_type,
	t1.search_agent_type,
	t1.device_type,
	t1.agent_details,
	t2a.session_cntry_id,
	coalesce(t2a.session_rev_rollup,'UNKN') AS session_rev_rollup,
	t2a.ip,
	t1.gr_cnt,
	t1.gr_1_cnt,
	t1.vi_cnt,
	t1.homepage_cnt,
	t1.myebay_cnt,
	t1.signin_cnt,
	t1.siid_cnt,
	t1.nonjs_hp_cnt,
	t1.primary_app_id,
	CASE
		WHEN t1.bot_flags64 = 0
			THEN 0
		ELSE 1
	END AS bot_session,
	t1.bot_flags64,
	t1.session_flags64,
	CAST(NULL AS SMALLINT) AS session_traffic_source_id,
	CAST(NULL AS STRING) AS session_traffic_source_details,
	CAST(NULL AS INTEGER) AS first_seqnum,
	CAST(0 AS TINYINT) AS on_ebay_sess,
	CAST(NULL AS STRING) AS session_traffic_source_dtl2,
	CASE WHEN t1.device_type IS NULL
			THEN ''
		ELSE '&dt=' || t1.device_type
	END
	|| CASE WHEN t1.lndg_mppid IS NULL
				THEN ''
			ELSE '&mppid=' || t1.lndg_mppid
        END
    || CASE WHEN t1.lndg_mnt IS NULL
                THEN ''
            ELSE '&mnt=' || t1.lndg_mnt
        END
    || CASE WHEN t1.lndg_ort IS NULL
                THEN ''
            ELSE '&ort=' || t1.lndg_ort
        END
    || CASE WHEN t1.override_guid IS NULL
                THEN ''
            ELSE '&ovrg=' || t1.override_guid
        END AS session_details,
	CAST(NULL AS STRING) AS ref_domain,
	t1.lndg_referrer AS referrer,
	CAST(NULL AS STRING) AS roverentry_src_string,
	CAST(NULL AS STRING) AS roverns_src_string,
	CAST(NULL AS STRING) AS roveropen_src_string,
	CAST(NULL AS STRING) AS rtm_src_string,
	CAST(NULL AS STRING) AS notif_src_string,
	CAST(NULL AS STRING) AS nvts_src_string,
	CAST(NULL AS STRING) AS ncts_src_string,
	CAST(NULL AS STRING) AS ndts_src_string,
	t1.lndg_page_id,
	CAST(NULL AS STRING) AS lndg_page_fmly4,
	t1.lndg_page_url,
	CAST(NULL AS STRING) AS lndg_sid,
	CAST(NULL AS STRING) AS lndg_mppid,
	CAST(NULL AS STRING) AS lndg_mnt,
	CAST(NULL AS STRING) AS lndg_ort,
	CAST(NULL AS STRING) AS ref_keyword,
	t1.start_timestamp,
	t1.end_timestamp,
	t1.exit_page_id,
	CAST(NULL AS TIMESTAMP) AS roverentry_ts,
	CAST(NULL AS TIMESTAMP) AS roverns_ts,
	CAST(NULL AS TIMESTAMP) AS roveropen_ts,
	CAST(NULL AS TIMESTAMP) AS rtm_ts,
	CAST(NULL AS TIMESTAMP) AS notification_ts,
	t1.lndg_notification_id AS notification_id,
	CAST(NULL AS TINYINT) AS debug_flag,
	CAST(NULL AS STRING) AS mcs_entry_src_string,
	CAST(NULL AS TIMESTAMP) AS mcs_entry_ts,
	CAST(NULL AS TIMESTAMP) AS updated_ts,
	CAST(NULL AS STRING) AS dlk_entry_src_string,
	CAST(NULL AS TIMESTAMP) AS dlk_entry_ts,
	CAST(NULL AS STRING) AS dlk_brguid,
	CAST(NULL AS BIGINT) AS dlk_brsess,
	CAST(NULL AS STRING) AS dlk_details,
	CAST(NULL AS STRING) AS dlk_deeplink,
	CAST(NULL AS STRING) AS dlk_referrer,
	CAST(NULL AS SMALLINT) AS dlk_mweb_link_type,
	CAST(NULL AS STRING) AS entry_event_src_string,
	CAST(NULL AS TIMESTAMP) AS entry_event_ts,
	CAST(NULL AS STRING) AS ns_event_src_string,
	CAST(NULL AS TIMESTAMP) AS ns_event_ts,
	CAST(NULL AS STRING) AS entry_string,
	CAST(NULL AS TIMESTAMP) AS lndg_page_ts
FROM
	p_soj_cl_t.temp_csess1k_v9 t1
	INNER JOIN p_soj_cl_t.temp_csess2a1_v9 t2a
	ON t1.guid = t2a.guid AND t1.session_skey = t2a.session_skey AND t1.site_id = t2a.site_id AND t1.cobrand = t2a.cobrand AND t1.session_start_dt = t2a.session_start_dt;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a_v9 t2a,
	p_soj_cl_v.pages pg
SET
	t2a.lndg_page_fmly4 = pg.page_fmly4_name
WHERE
	t2a.lndg_page_id = pg.page_id;

UPDATE p_soj_cl_t.temp_csess2a_v9
SET
	mapped_user_id = NULL,
	session_details = session_details || '&mhpbu=' || CAST(mapped_user_id AS STRING)
WHERE
	lndg_page_id = 2481888
	AND primary_app_id = 3564
	AND mapped_user_id IS NOT NULL
	AND valid_page_count BETWEEN 1 AND 2
	AND exit_page_id = 2481888;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;