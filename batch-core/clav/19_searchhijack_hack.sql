DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13h_v9;
CREATE TABLE p_soj_cl_t.temp_csess13h_v9 USING delta AS
SELECT
	s.guid,
	s.session_skey,
	s.site_id,
	s.session_start_dt,
	s.cobrand,
	s.primary_app_id,
	s.ip,
	gr.page_id,
	gr.external_ip,
	gr.user_id,
	gr.best_guess_user_id,
	gr.event_timestamp,
	gr.seqnum,
	gr.sqr2,
	gr.bnid2,
	CAST(NULL AS STRING) AS app_match_guid,
	CAST(NULL AS DECIMAL(18,0)) AS app_match_sskey,
	CAST(NULL AS INTEGER) AS app_match_type,
	SUM(1) OVER (PARTITION BY s.guid, s.session_skey, s.site_id, s.session_start_dt ORDER BY gr.seqnum ROWS Unbounded PRECEDING) AS dedupe_rank
FROM
	p_soj_cl_t.temp_csess1b1_v9 gr
	INNER JOIN p_soj_cl_v.pages pg
	ON (gr.page_id = pg.page_id) and pg.page_fmly4_name IN ('GR','GR-1')
	INNER JOIN p_soj_cl_t.temp_csess2a_v9 s
	ON (s.guid = gr.guid AND s.session_skey = gr.session_skey AND s.site_id = gr.site_id AND s.session_start_dt = gr.session_start_dt)
WHERE
	gr.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND s.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01' AND s.cobrand IN (7,6) AND COALESCE(s.primary_app_id,3564) = 3564
QUALIFY dedupe_rank = 1;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13i_v9;
CREATE TABLE p_soj_cl_t.temp_csess13i_v9 USING PARQUET AS
SELECT
	t2.guid AS dlk_guid,
	t2.session_skey AS dlk_sskey,
	t2.site_id,
	t2.ip,
	CAST(sojlib.soj_nvl(t2.dlk_entry_src_string, 't') AS TIMESTAMP) AS dlk_timestamp,
	CAST(replace(sojlib.soj_nvl(dlk_details,'sqr'),'+',' ') AS STRING) dlk_sqr
FROM
	p_soj_cl_t.temp_csess2a_v9 t2
WHERE
	t2.dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(t2.dlk_details,'dltype') <> 'sab' AND dlk_brguid IS NULL AND sojlib.soj_nvl(dlk_details,'sqr') IS NOT NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t2.dlk_guid,
			t2.dlk_sskey,
			t2.site_id,
			t2.dlk_timestamp,
			t2.ip,
			t13h.external_ip,
			t13h.guid AS mweb_guid,
			t13h.session_skey AS mweb_sskey,
			t13h.event_timestamp,
			SUM(1) OVER (PARTITION BY t2.dlk_guid, t2.dlk_sskey,t2.site_id ORDER BY t13h.event_timestamp ROWS Unbounded PRECEDING) AS dedupe_rank
		FROM
			p_soj_cl_t.temp_csess13i_v9 t2
			INNER JOIN p_soj_cl_t.temp_csess13h_v9 t13h
			ON (replace(t2.dlk_sqr,'+',' ') = t13h.sqr2 AND t13h.event_timestamp BETWEEN t2.dlk_timestamp - INTERVAL '30' SECOND AND t2.dlk_timestamp + INTERVAL '10' SECOND)
		WHERE t13h.sqr2 IS NOT NULL AND t2.ip = t13h.external_ip
		QUALIFY dedupe_rank = 1
	) gr
SET
	t2.dlk_brguid = gr.mweb_guid,
	t2.dlk_brsess = gr.mweb_sskey,
	t2.dlk_mweb_link_type = 6
WHERE
	gr.dlk_guid = t2.guid AND gr.dlk_sskey = t2.session_skey AND gr.site_id = t2.site_id AND dlk_brguid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t2.dlk_guid,
			t2.dlk_sskey,
			t2.site_id,
			t2.dlk_timestamp,
			MIN(t13h.guid) AS mweb_guid1,
			MIN(t13h.session_skey) AS mweb_sskey1,
			MAX(t13h.guid) AS mweb_guid2,
			MAX(t13h.session_skey) AS mweb_sskey2
		FROM
			p_soj_cl_t.temp_csess13i_v9 t2
			INNER JOIN p_soj_cl_t.temp_csess13h_v9 t13h
			ON (t2.dlk_sqr = t13h.sqr2 AND t13h.event_timestamp BETWEEN t2.dlk_timestamp - INTERVAL '15' SECOND AND t2.dlk_timestamp + INTERVAL '5' SECOND)
		WHERE t13h.sqr2 IS NOT NULL
		GROUP BY 1,2,3,4
		HAVING mweb_guid1 = mweb_guid2 AND mweb_sskey1 = mweb_sskey2
	) gr
SET
	t2.dlk_brguid = gr.mweb_guid1,
	t2.dlk_brsess = gr.mweb_sskey1,
	t2.dlk_mweb_link_type = 7
WHERE
	gr.dlk_guid = t2.guid AND gr.dlk_sskey = t2.session_skey AND gr.site_id = t2.site_id AND dlk_brguid IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13j_v9;
CREATE TABLE p_soj_cl_t.temp_csess13j_v9 USING PARQUET AS
SELECT
	t2.guid AS dlk_guid,
	t2.session_skey AS dlk_sskey,
	t2.site_id,
	t2.ip,
	CAST(sojlib.soj_nvl(t2.dlk_entry_src_string, 't') AS TIMESTAMP) AS dlk_timestamp,
	CAST(sojlib.soj_nvl(dlk_details,'bnid') as DECIMAL(18,0)) dlk_bnid
FROM
	p_soj_cl_t.temp_csess2a_v9 t2
WHERE
	t2.dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(t2.dlk_details,'dltype') <> 'sab' AND dlk_brguid IS NULL AND sojlib.soj_nvl(dlk_details,'bnid') IS NOT NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			dlk.dlk_guid,
			dlk.dlk_sskey,
			dlk.site_id,
			dlk.dlk_timestamp,
			dlk.ip,
			t13h.external_ip,
			t13h.guid AS mweb_guid,
			t13h.session_skey AS mweb_sskey,
			t13h.event_timestamp,
			SUM(1) OVER (PARTITION BY dlk.dlk_guid, dlk.dlk_sskey,dlk.site_id ORDER BY t13h.event_timestamp ROWS Unbounded PRECEDING) AS dedupe_rank
		FROM
			p_soj_cl_t.temp_csess13j_v9 dlk
			INNER JOIN p_soj_cl_t.temp_csess13h_v9 t13h
			ON (dlk.dlk_bnid = t13h.bnid2 AND t13h.event_timestamp BETWEEN dlk.dlk_timestamp - INTERVAL '30' SECOND AND dlk.dlk_timestamp + INTERVAL '10' SECOND)
		WHERE t13h.bnid2 IS NOT NULL AND dlk.ip = t13h.external_ip
		QUALIFY dedupe_rank = 1
	) gr
SET
	t2.dlk_brguid = gr.mweb_guid,
	t2.dlk_brsess = gr.mweb_sskey,
	t2.dlk_mweb_link_type = 8
WHERE
	gr.dlk_guid = t2.guid AND gr.dlk_sskey = t2.session_skey AND gr.site_id = t2.site_id AND dlk_brguid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			dlk.dlk_guid,
			dlk.dlk_sskey,
			dlk.site_id,
			dlk.dlk_timestamp,
			MIN(t13h.guid) AS mweb_guid1,
			MIN(t13h.session_skey) AS mweb_sskey1,
			MAX(t13h.guid) AS mweb_guid2,
			MAX(t13h.session_skey) AS mweb_sskey2
		FROM
			p_soj_cl_t.temp_csess13j_v9 dlk I
			NNER JOIN p_soj_cl_t.temp_csess13h_v9 t13h
			ON dlk.dlk_bnid = t13h.bnid2 AND t13h.event_timestamp BETWEEN dlk.dlk_timestamp - INTERVAL '5' SECOND AND dlk.dlk_timestamp + INTERVAL '1' SECOND
		WHERE t13h.bnid2 IS NOT NULL
		GROUP BY 1,2,3,4
		HAVING mweb_guid1 = mweb_guid2 AND mweb_sskey1 = mweb_sskey2
	) gr
SET
	t2.dlk_brguid = gr.mweb_guid1,
	t2.dlk_brsess = gr.mweb_sskey1,
	t2.dlk_mweb_link_type = 9
WHERE
	gr.dlk_guid = t2.guid AND gr.dlk_sskey = t2.session_skey AND gr.site_id = t2.site_id AND dlk_brguid IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13k_v9;
CREATE TABLE p_soj_cl_t.temp_csess13k_v9 USING PARQUET AS
SELECT
	t13h.guid,
	t13h.session_skey,
	t13h.sqr2,
	t13h.event_timestamp,
	t13h.user_id
FROM
	p_soj_cl_t.temp_csess13h_v9 t13h
WHERE
	t13h.sqr2 IS NOT NULL AND t13h.app_match_guid IS NULL AND t13h.user_id IS NOT NULL;

UPDATE t13h
FROM
	p_soj_cl_t.temp_csess13h_v9 t13h,
	(
		SELECT
			t13h.guid,
			t13h.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			t13h.sqr2,
			t13h.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			Sum(1) Over(PARTITION BY t13h.guid, t13h.session_skey, t13h.sqr2, t13h.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded Preceding) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13h_v9 t13h
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON t13h.user_id = dlk.user_id AND replace(dlk.dlk_sqr,'+',' ') = t13h.sqr2 AND dlk.event_timestamp BETWEEN t13h.event_timestamp - INTERVAL '5' SECOND AND t13h.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	t13h.app_match_guid = dlk.dlk_guid,
	t13h.app_match_sskey = dlk.dlk_sskey,
	t13h.app_match_type = 3
WHERE
	t13h.guid = dlk.guid AND t13h.session_skey = dlk.session_skey AND t13h.event_timestamp = dlk.event_timestamp AND t13h.app_match_guid IS NULL;

UPDATE t13h
FROM
	p_soj_cl_t.temp_csess13h_v9 t13h,
	(
		SELECT
			t.guid,
			t.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			t.sqr2,
			t.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY t.guid, t.session_skey, t.sqr2, t.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13h_v9 t
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON t.sqr2 = dlk.dlk_sqr AND t.external_ip = dlk.external_ip AND dlk.event_timestamp BETWEEN t.event_timestamp - INTERVAL '5' SECOND AND t.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	t13h.app_match_guid = dlk.dlk_guid,
	t13h.app_match_sskey = dlk.dlk_sskey,
	t13h.app_match_type = 3 /* sqr */
WHERE
	t13h.guid = dlk.guid AND t13h.session_skey = dlk.session_skey AND t13h.event_timestamp = dlk.event_timestamp AND t13h.app_match_guid IS NULL;

UPDATE t13h
FROM
	p_soj_cl_t.temp_csess13h_v9 t13h,
	(
		SELECT
			t.guid,
			t.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			t.bnid2,
			t.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY t.guid, t.session_skey, t.bnid2, t.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13h_v9 t
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON t.user_id = dlk.user_id AND t.bnid2 = dlk.dlk_bnid AND dlk.event_timestamp BETWEEN t.event_timestamp - INTERVAL '5' SECOND AND t.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	t13h.app_match_guid = dlk.dlk_guid,
	t13h.app_match_sskey = dlk.dlk_sskey,
	t13h.app_match_type = 4 /*bnid*/
WHERE
	t13h.guid = dlk.guid AND t13h.session_skey = dlk.session_skey AND t13h.event_timestamp = dlk.event_timestamp AND t13h.app_match_guid IS NULL;

UPDATE t13h
FROM
	p_soj_cl_t.temp_csess13h_v9 t13h,
	(
		SELECT
			t.guid,
			t.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			t.bnid2,
			t.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY t.guid, t.session_skey, t.bnid2, t.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13h_v9 t
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON t.bnid2 = dlk.dlk_bnid AND t.external_ip = dlk.external_ip AND dlk.event_timestamp BETWEEN t.event_timestamp - INTERVAL '5' SECOND AND t.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	t13h.app_match_guid = dlk.dlk_guid,
	t13h.app_match_sskey = dlk.dlk_sskey,
	t13h.app_match_type = 4 /*bnid */
WHERE
	t13h.guid = dlk.guid AND t13h.session_skey = dlk.session_skey AND t13h.event_timestamp = dlk.event_timestamp AND t13h.app_match_guid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t13h.guid,
			t13h.session_skey,
			t13h.session_start_dt,
			t13h.site_id,
			t13h.cobrand,
			t13h.primary_app_id,
			t13h.event_timestamp,
			t13h.app_match_guid,
			t13h.app_match_sskey,
			t13h.app_match_type,
			SUM(1) OVER(PARTITION BY t13h.guid, t13h.session_skey, t13h.session_start_dt, t13h.site_id, t13h.cobrand ORDER BY t13h.event_timestamp ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13h_v9 t13h
		WHERE
			t13h.app_match_guid IS NOT NULL
		QUALIFY first_rank = 1
	) dlk
SET
	t2.session_details = COALESCE(session_details, '') || '&appguid=' || CAST(dlk.app_match_guid AS STRING) || '&appsess=' || CAST(dlk.app_match_sskey AS STRING) || '&applink=' || CAST(dlk.app_match_type AS STRING)
WHERE
	dlk.guid = t2.guid AND dlk.session_skey = t2.session_skey AND dlk.session_start_dt = t2.session_start_dt AND dlk.site_id = t2.site_id AND dlk.cobrand = t2.cobrand AND sojlib.soj_nvl(t2.session_details,'appguid') IS NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;