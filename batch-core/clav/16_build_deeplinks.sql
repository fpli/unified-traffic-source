DROP TABLE IF EXISTS p_soj_cl_t.temp_csess12aa_v9;
CREATE TABLE p_soj_cl_t.temp_csess12aa_v9 USING delta AS
SELECT
	*
FROM
	p_soj_cl_t.deeplink_dtl_w1
WHERE
	session_start_dt BETWEEN '2024-05-31' AND '2024-06-01';

UPDATE t12a
FROM
	p_soj_cl_t.temp_csess12aa_v9 t12a,
	(
		SELECT
			dl.guid,
			dl.site_id,
			dl.event_timestamp,
			dl.seqnum,
			MIN(s.session_skey) session_skey
		FROM
			p_soj_cl_t.temp_csess12aa_v9 dl
			INNER JOIN p_soj_cl_t.temp_csess2a_v9 s
			ON s.guid = dl.sabg AND dl.event_timestamp BETWEEN s.start_timestamp - INTERVAL '30' MINUTE AND s.start_timestamp + INTERVAL '30' MINUTE
		WHERE
			dl.dl_type = 'sab' AND dl.sabg IS NOT NULL AND dl.sabs IS NULL
		GROUP BY 1,2,3,4
	) s
SET
	t12a.sabs = s.session_skey,
	t12a.link_src = 0 /* sab with mweb session*/
WHERE
	t12a.guid = s.guid AND t12a.event_timestamp = s.event_timestamp AND t12a.seqnum = s.seqnum AND t12a.dl_type = 'sab';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess12b_v9;
CREATE TABLE p_soj_cl_t.temp_csess12b_v9 USING PARQUET AS
SELECT
	guid,
	session_skey,
	site_iD,
	session_start_dt,
	ip,
	COALESCE(signedin_user_id, mapped_user_id) as user_id,
	app_id,
	seqnum,
	event_timestamp,
	CAST(sojlib.soj_nvl(first_ip, 'ip') AS STRING) AS dlk_ip,
	CAST(sojlib.soj_nvl(first_gclid, 'cglid') AS STRING) AS dlk_gclid,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_ref, 'ref'),'%') AS STRING) AS dlk_referrer,
	CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(first_dlk, 'dlk'),'%') AS STRING) AS dlk_deeplink,
	CAST(sojlib.soj_nvl(first_itm, 'itm') AS DECIMAL(18,0)) AS dlk_item_id,
	CAST(sojlib.soj_nvl(first_epid, 'epid') AS DECIMAL(18,0)) AS dlk_epid,
	CAST(sojlib.soj_nvl(first_sqr, 'sqr') AS STRING) AS dlk_sqr,
	CAST(sojlib.soj_nvl(first_bnid, 'bnid') AS DECIMAL(18,0)) AS dlk_bnid,
	CASE WHEN sojlib.soj_nvl(dlk_sab, 'sabg') IS NOT NULL THEN CAST(sojlib.soj_nvl(dlk_sab, 'sabg') AS STRING) ELSE NULL END AS dlk_mweb_guid,
	CASE WHEN sojlib.soj_nvl(dlk_sab, 'sabs') IS NOT NULL THEN CAST(sojlib.soj_nvl(dlk_sab, 'sabs') AS STRING) ELSE NULL END AS dlk_mweb_sess,
	CASE WHEN dlk_sab IS NOT NULL THEN 0 ELSE NULL END AS dlk_mweb_link_type,
	COALESCE(first_src_string,'')
		|| CASE WHEN sojlib.soj_nvl(first_src_string,'rvr') IS NULL AND sojlib.soj_nvl(first_rvrid,'rvr') IS NOT NULL AND sojlib.soj_nvl(first_rvrid,'rvr') <> '0' THEN '&rvr=' || CAST(CAST(sojlib.soj_nvl(first_rvrid,'rvr') AS BIGINT) AS STRING) ELSE '' END
		|| CASE WHEN sojlib.soj_nvl(first_mpc,'mpc') IS NULL THEN '' ELSE '&mpc=' || CAST(sojlib.soj_nvl(first_mpc,'mpc') AS STRING) END
		|| CASE WHEN sojlib.soj_nvl(first_rc,'rc') IS NULL THEN '' ELSE '&rc=' || CAST(sojlib.soj_nvl(first_rc,'rc') AS STRING) END
		|| CASE WHEN sojlib.soj_nvl(first_crlp,'crlp') IS NULL THEN '' ELSE '&crlp=' || CAST(sojlib.soj_nvl(first_crlp,'crlp') AS STRING) END
		|| CASE WHEN sojlib.soj_nvl(first_geo,'geo') IS NULL THEN '' ELSE '&geo=' ||CAST(sojlib.soj_nvl(first_geo,'geo') AS STRING) END
		|| CASE WHEN sojlib.soj_nvl(first_bk,'bk') IS NULL THEN '' ELSE '&bk=' || CAST(sojlib.soj_nvl(first_bk,'bk') AS STRING) END
		|| CAST(CASE WHEN sojlib.soj_nvl(first_camp,'camp') IS NOT NULL AND sojlib.is_decimal(sojlib.soj_nvl(first_camp,'camp'), 18, 0) = 1 THEN '&camp=' || sojlib.soj_nvl(first_camp,'camp') ELSE '' END AS STRING) AS dlk_src_string,
	CAST(sojlib.soj_nvl(COALESCE(first_dlt1, first_dlt2), 'dl_type') AS STRING) AS dlk_type
FROM (
	SELECT
		t2a.guid,
		t2a.session_skey,
		t2a.site_id,
		t2a.session_start_dt,
		t2a.ip,
		MIN(t12a.app_id) as app_id,
		MIN(t2a.signedin_user_id) AS signedin_user_id,
		MIN(t2a.mapped_user_id) AS mapped_user_id,
		MIN(t12a.seqnum) AS seqnum,
		MIN(t12a.event_timestamp) AS event_timestamp,
		MIN(CASE WHEN t12a.external_ip IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')|| '&ip=' || t12a.external_ip END) AS first_ip,
		MIN(CASE WHEN t12a.gclid IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&cglid=' || t12a.gclid END) AS first_gclid,
		MIN(CASE WHEN t12a.referrer IS NULL THEN NULL WHEN t12a.referrer = 'null' THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&ref=' || CAST(TRIM(replace(replace(replace(t12a.referrer, '%', '%25'), '&', '%26'), '=', '%3D')) AS STRING) END) AS first_ref,
		MIN(CASE WHEN t12a.deeplink LIKE ANY ('https://rover%') THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&dlk=' || CAST(TRIM(replace(replace(replace(t12a.deeplink, '%', '%25'), '&', '%26'), '=', '%3D')) AS STRING) END) AS first_dlk,
		MIN(CASE WHEN t12a.dlk_itm IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')|| '&itm=' || CAST(t12a.dlk_itm AS STRING) END) AS first_itm,
		MIN(CASE WHEN t12a.dlk_epid IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&epid=' || CAST(t12a.dlk_epid AS STRING) END) AS first_epid,
		MIN(CASE WHEN t12a.dlk_sqr IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')|| '&sqr=' || CAST(t12a.dlk_sqr AS STRING) END) AS first_sqr,
		MIN(CASE WHEN t12a.dlk_bnid IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&bnid=' || CAST(t12a.dlk_bnid AS STRING) END) AS first_bnid,
		MIN(CASE WHEN t12a.rvrid IS NULL THEN NULL WHEN t12a.rvrid = 0 THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&rvr=' || CAST(t12a.rvrid AS STRING) END) AS first_rvrid,
		MIN(CASE WHEN t12a.sabg IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&sabg=' || CAST(t12a.sabg AS STRING) END || CASE WHEN t12a.sabc IS NULL THEN '' else '&sabc=' || CAST(t12a.sabc AS STRING) END || CASE WHEN t12a.sabs IS NULL THEN '' else '&sabs=' || CAST(t12a.sabs AS STRING) END) AS dlk_sab,
		MIN(CASE WHEN t12a.src_string IS NULL THEN NULL ELSE t12a.src_string END) AS first_src_string,
		MIN(CASE WHEN t12a.rotation_id IS NULL THEN NULL WHEN t12a.rotation_id = 0 THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&rot=' || CAST(CAST(t12a.rotation_id AS BIGINT) AS STRING) END) AS first_rot,
		MIN(CASE WHEN t12a.channel IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&rc=' || CAST(t12a.channel AS STRING) END) AS first_rc,
		MIN(CASE WHEN t12a.mpx_chnl_id IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')|| '&mpc=' || CAST(t12a.mpx_chnl_id AS STRING) END) AS first_mpc,
		MIN(CASE WHEN t12a.campaign_id IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&camp=' || CAST(t12a.campaign_id AS STRING) END) AS first_camp,
		MIN(CASE WHEN t12a.bought_keyword IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')|| '&bk=' || CAST(t12a.bought_keyword AS STRING) END) AS first_bk,
		MIN(CASE WHEN t12a.crlp IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&crlp=' || CAST(t12a.crlp AS STRING) END) AS first_crlp,
		MIN(CASE WHEN t12a.geo_id IS NULL THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&geo=' || CAST(t12a.geo_id AS STRING) END) AS first_geo,
		MAX(CASE WHEN dl_type IN ('nhubactn','nactn') THEN 1 ELSE 0 end) AS notif_ind,
		MIN(CASE WHEN dl_type = 'other' THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&dl_type=' || dl_type END) AS first_dlt1,
		MIN(CASE WHEN dl_type <> 'other' THEN NULL ELSE 'ts=' || date_format(CAST(CAST(t12a.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&dl_type=' || dl_type END) AS first_dlt2
	FROM
		p_soj_cl_t.temp_csess2a_v9 t2a
		INNER JOIN p_soj_cl_t.temp_csess12aa_v9 t12a
		ON (t2a.guid = t12a.guid AND t2a.session_skey = t12a.session_skey AND t2a.session_start_dt = t12a.session_start_dt AND t12a.event_timestamp BETWEEN t2a.start_timestamp - INTERVAL '30' SECOND AND t2a.start_timestamp + INTERVAL '15' SECOND)
		GROUP BY 1,2,3,4,5
		HAVING notif_ind = 0
	) dl;