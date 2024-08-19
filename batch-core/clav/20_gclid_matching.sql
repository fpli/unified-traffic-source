DROP TABLE IF EXISTS p_soj_cl_t.temp_csess17aa_v9;
CREATE TABLE p_soj_cl_t.temp_csess17aa_v9 USING PARQUET AS
SELECT
	guid,
	session_skey,
	session_start_dt,
	site_id,
	cguid,
	partial_valid_page,
	rdt,
	event_timestamp,
	page_id,
	CASE
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'gclid2') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'gclid2')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'ul_ref'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'ul_ref'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'ul_ref'),'%'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%')),'ul_ref'),'%'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ru'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ru'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'loc'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'loc'),'%')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%'),'%')),'loc')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'ul_ref'),'%'),'%'),'%')),'loc')),'gclid')
		WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'winLoc'),'%')),'gclid') IS NOT NULL
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(page_url),'winLoc'),'%')),'gclid')
		ELSE NULL
	END as gclid
FROM
	p_soj_cl_t.temp_csess1b1_v9
WHERE
	page_url LIKE '%gclid%' AND page_url NOT LIKE ALL ('%gclid=&%','%gclid%7D&%','%gclid=')
GROUP by 1,2,3,4,5,6,7,8,9,10;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess17a_v9;
CREATE TABLE p_soj_cl_t.temp_csess17a_v9 USING delta AS
SELECT
	g.gclid,
	g.guid,
	g.session_skey,
	g.session_start_dt,
	g.cguid,
	g.site_id,
	g.partial_valid_page,
	g.rdt,
	g.event_timestamp,
	g.page_id,
	CAST(NULL AS STRING) AS app_match_guid,
	CAST(NULL AS BIGINT) AS app_match_sskey,
	CAST(NULL AS SMALLINT) AS app_match_type,
	SUM(1) OVER(PARTITION BY g.gclid, g.guid, g.session_skey ORDER BY g.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
FROM
	p_soj_cl_t.temp_csess17aa_v9 g
	INNER JOIN p_soj_cl_t.temp_csess2a_v9 t2
	ON g.guid = t2.guid AND g.session_skey = t2.session_skey AND g.session_start_dt = t2.session_start_dt AND g.site_id = t2.site_id
WHERE
	t2.cobrand IN (6,7) AND COALESCE(t2.primary_app_id,3564) = 3564 AND g.gclid IS NOT NULL AND LENGTH(g.gclid) > 50
QUALIFY first_rank = 1;

UPDATE g
FROM
	p_soj_cl_t.temp_csess17a_v9 g,
	(
		SELECT
			g.*,
			d.guid AS dlk_guid,
			d.session_skey AS dlk_sskey,
			d.event_timestamp AS dlk_event_ts,
			SUM(1) OVER(PARTITION BY g.guid, g.session_skey ORDER BY d.event_timestamp ASC ROWS Unbounded PRECEDING) AS fst_rank
		FROM
			p_soj_cl_t.temp_csess17a_v9 g
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 d
			ON g.gclid = d.gclid AND d.event_timestamp BETWEEN g.event_timestamp - INTERVAL '15' SECOND AND g.event_timestamp + INTERVAL '15' SECOND
		QUALIFY fst_rank = 1
	) d
SET
	g.app_match_guid = d.dlk_guid,
	g.app_match_sskey = d.dlk_sskey,
	g.app_match_type = 5
WHERE
	g.guid = d.guid AND g.session_skey = d.session_skey AND g.site_id = d.site_id AND g.session_start_dt = d.session_start_dt;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t17a.guid,
			t17a.session_skey,
			t17a.session_start_dt,
			t17a.site_id,
			t17a.app_match_guid,
			t17a.app_match_sskey,
			t17a.app_match_type,
			t17a.event_timestamp,
			SUM(1) OVER(PARTITION BY t17a.guid,t17a.session_skey,t17a.session_start_dt,t17a.site_id ORDER BY t17a.event_timestamp ASC ROWS Unbounded PRECEDING) AS rank1
		FROM
			p_soj_cl_t.temp_csess17a_v9 t17a
		QUALIFY rank1 = 1
	) t17a
SET
	t2.session_details = COALESCE(session_details, '') || '&appguid=' || CAST(t17a.app_match_guid AS STRING) || '&appsess=' || CAST(t17a.app_match_sskey AS STRING) || '&applink=' || CAST(t17a.app_match_type AS STRING)
WHERE
	t17a.guid = t2.guid AND t17a.session_skey = t2.session_skey AND t17a.session_start_dt = t2.session_start_dt AND t17a.site_id = t2.site_id AND sojlib.soj_nvl(t2.session_details,'appguid') IS NULL AND t2.cobrand IN (6,7);


UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			g.guid,
			g.session_skey,
			g.session_start_dt,
			g.event_timestamp,
			g.page_id,
			d.guid AS dlk_guid,
			d.session_skey AS dlk_sskey,
			d.site_id AS dlk_site_id,
			d.dlk_entry_ts AS dlk_event_ts,
			SUM(1) Over(PARTITION BY d.guid, d.session_skey ORDER BY g.event_timestamp ROWS Unbounded Preceding) AS fst_rank
		FROM
			p_soj_cl_t.temp_csess17a_v9 g /* mweb gclid pages */
			INNER JOIN (
							SELECT
								guid,
								session_skey,
								site_id,
								dlk_entry_ts,
								CAST(sojlib.soj_nvl(dlk_details,'gclid') AS STRING) AS gclid
							FROM
								p_soj_cl_t.temp_csess2a_v9 s
							WHERE
								dlk_entry_ts IS NOT NULL AND sojlib.soj_nvl(dlk_details,'gclid') IS NOT NULL AND dlk_brguid IS NULL
					   ) d
					   ON g.gclid = d.gclid AND d.dlk_entry_ts BETWEEN g.event_timestamp - INTERVAL '15' SECOND AND g.event_timestamp + INTERVAL '15' SECOND
		QUALIFY fst_rank = 1
	) d
SET
	t2.dlk_brguid = d.guid,
	t2.dlk_brsess = d.session_skey,
	t2.dlk_mweb_link_type = 10 /*gclid*/
WHERE
	d.dlk_guid = t2.guid AND d.dlk_sskey = t2.session_skey AND d.dlk_site_id = t2.site_id AND dlk_brguid IS NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;