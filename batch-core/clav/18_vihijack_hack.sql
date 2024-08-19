DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13a_v9;
CREATE TABLE p_soj_cl_t.temp_csess13a_v9 USING delta AS
SELECT
	s.guid,
	s.session_skey,
	s.site_id,
	s.session_start_dt,
	s.cobrand,
	s.primary_app_id,
	s.ip as session_ip,
	vi.page_id,
	vi.external_ip as page_ip,
	vi.user_id,
	vi.best_guess_user_id,
	vi.event_timestamp,
	vi.seqnum,
	vi.item_id,
	vi.epid,
	CAST(NULL AS STRING) AS app_match_guid,
	CAST(NULL AS DECIMAL(18,0)) AS app_match_sskey,
	CAST(NULL AS INTEGER) AS app_match_type
FROM
	p_soj_cl_t.temp_csess1b1_v9 vi
	INNER JOIN p_soj_cl_v.pages pg
	ON (vi.page_id = pg.page_id AND pg.page_fmly4_name IN ('VI','GR/VI'))
	INNER JOIN p_soj_cl_t.temp_csess2a_v9 s
	ON (s.guid = vi.guid AND s.session_skey = vi.session_skey AND s.site_id = vi.site_id AND s.session_start_dt = vi.session_start_dt)
WHERE
	vi.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND s.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND s.cobrand IN (6,7)
	AND COALESCE(s.primary_app_id, 3564) = 3564;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13b_v9;
CREATE TABLE p_soj_cl_t.temp_csess13b_v9 USING PARQUET AS
SELECT
    *
FROM
    p_soj_cl_t.temp_csess13a_v9
WHERE
    item_id IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13c_v9;
CREATE TABLE p_soj_cl_t.temp_csess13c_v9 USING PARQUET AS
SELECT
    t2.guid AS dlk_guid,
    t2.session_skey AS dlk_sskey,
    t2.site_id,
    t2.ip,
    CAST(sojlib.soj_nvl(t2.dlk_entry_src_string, 't') AS TIMESTAMP) as dlk_timestamp,
    CAST(sojlib.soj_nvl(t2.dlk_details,'itm') AS DECIMAL(18,0)) AS dlk_itm
FROM
    p_soj_cl_t.temp_csess2a_v9 t2
WHERE
    sojlib.soj_nvl(t2.dlk_details,'itm') IS NOT NULL AND sojlib.soj_nvl(t2.dlk_details,'dltype') <> 'sab' AND dlk_brguid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t13c.dlk_guid,
			t13c.dlk_sskey,
			t13c.site_id,
			t13c.dlk_itm,
			t13c.dlk_timestamp,
			t13b.session_ip,
			t13b.guid AS mweb_guid,
			t13b.session_skey AS mweb_sskey,
			t13b.event_timestamp,
			SUM(1) OVER (PARTITION BY t13c.dlk_guid, t13c.dlk_sskey, t13c.site_id ORDER BY t13b.event_timestamp ROWS UNBOUNDED PRECEDING) AS dedupe_rank
		FROM
			p_soj_cl_t.temp_csess13c_v9 t13c
			INNER JOIN p_soj_cl_t.temp_csess13b_v9 t13b
			ON t13c.dlk_itm = t13b.item_id AND t13b.event_timestamp BETWEEN t13c.dlk_timestamp - INTERVAL '30' SECOND AND t13c.dlk_timestamp + INTERVAL '10' SECOND
		WHERE
			t13b.item_id IS NOT NULL AND t13c.ip = t13b.session_ip
		QUALIFY dedupe_rank = 1
	) vi
SET
	t2.dlk_brguid = vi.mweb_guid,
	t2.dlk_brsess = vi.mweb_sskey,
	t2.dlk_mweb_link_type = 2
WHERE
	vi.dlk_guid = t2.guid AND vi.dlk_sskey = t2.session_skey AND vi.site_id = t2.site_id AND dlk_brguid IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13d_v9;
CREATE TABLE p_soj_cl_t.temp_csess13d_v9 USING PARQUET AS
SELECT
	dlk.dlk_guid,
	dlk.dlk_sskey,
	dlk.site_id,
	dlk.dlk_itm,
	dlk.dlk_timestamp,
	MIN(vi.guid) AS mweb_guid1,
	MIN(vi.session_skey) AS mweb_sskey1,
	MAX(vi.guid) AS mweb_guid2,
	MAX(vi.session_skey) AS mweb_sskey2
FROM
	p_soj_cl_t.temp_csess13c_v9 dlk
	INNER JOIN p_soj_cl_t.temp_csess13b_v9 vi
	ON dlk.dlk_itm = vi.item_id AND vi.event_timestamp BETWEEN dlk.dlk_timestamp - INTERVAL '15' SECOND AND dlk.dlk_timestamp + INTERVAL '5' SECOND
WHERE
	vi.item_id IS NOT NULL
GROUP BY 1,2,3,4,5
HAVING mweb_guid1 = mweb_guid2 AND mweb_sskey1 = mweb_sskey2;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	p_soj_cl_t.temp_csess13d_v9 vi
SET
	t2.dlk_brguid = vi.mweb_guid1,
	t2.dlk_brsess = vi.mweb_sskey1,
	t2.dlk_mweb_link_type = 3
WHERE
	vi.dlk_guid = t2.guid AND vi.dlk_sskey = t2.session_skey AND vi.site_id = t2.site_id AND dlk_brguid IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13e_v9;
CREATE TABLE p_soj_cl_t.temp_csess13e_v9 USING PARQUET AS
SELECT
	*
FROM
	p_soj_cl_t.temp_csess13a_v9
WHERE
	epid IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess13f_v9;
CREATE TABLE p_soj_cl_t.temp_csess13f_v9 USING PARQUET AS
SELECT
	t2.guid AS dlk_guid,
	t2.session_skey AS dlk_sskey,
	t2.site_id,
	t2.ip,
	CAST(sojlib.soj_nvl(t2.dlk_entry_src_string, 't') AS TIMESTAMP) as dlk_timestamp,
	CAST(sojlib.soj_nvl(t2.dlk_details,'epid') AS DECIMAL(18,0)) as dlk_epid
FROM
	p_soj_cl_t.temp_csess2a_v9 t2
WHERE
	sojlib.soj_nvl(t2.dlk_details,'epid') IS NOT NULL AND sojlib.soj_nvl(t2.dlk_details,'dltype') <> 'sab' AND dlk_brguid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t2.dlk_guid,
			t2.dlk_sskey,
			t2.site_id,
			t2.dlk_epid,
			t2.dlk_timestamp,
			t13e.session_ip,
			t13e.guid AS mweb_guid,
			t13e.session_skey AS mweb_sskey,
			t13e.event_timestamp,
			SUM(1) OVER (PARTITION BY t2.dlk_guid, t2.dlk_sskey, t2.site_id ORDER BY t13e.event_timestamp ROWS Unbounded PRECEDING) AS dedupe_rank
		FROM
			p_soj_cl_t.temp_csess13f_v9 t2
			INNER JOIN p_soj_cl_t.temp_csess13e_v9 t13e
			ON t2.dlk_epid = t13e.epid AND t13e.event_timestamp BETWEEN t2.dlk_timestamp - INTERVAL '30' SECOND AND t2.dlk_timestamp + INTERVAL '10' SECOND
		WHERE
		    t13e.epid IS NOT NULL AND t2.ip = t13e.session_ip
	    QUALIFY dedupe_rank = 1
	) vi
SET
	t2.dlk_brguid = vi.mweb_guid,
	t2.dlk_brsess = vi.mweb_sskey,
	t2.dlk_mweb_link_type = 4
WHERE
	vi.dlk_guid = t2.guid AND vi.dlk_sskey = t2.session_skey AND vi.site_id = t2.site_id AND dlk_brguid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t2.dlk_guid,
			t2.dlk_sskey,
			t2.site_id,
			t2.dlk_epid,
			t2.dlk_timestamp,
			MIN(t13e.guid) AS mweb_guid1,
			MIN(t13e.session_skey) AS mweb_sskey1,
			MAX(t13e.guid) AS mweb_guid2,
			MAX(t13e.session_skey) AS mweb_sskey2
		FROM
			p_soj_cl_t.temp_csess13f_v9 t2
			INNER JOIN p_soj_cl_t.temp_csess13e_v9 t13e
			ON t2.dlk_epid = t13e.epid AND t13e.event_timestamp BETWEEN t2.dlk_timestamp - INTERVAL '15' SECOND AND t2.dlk_timestamp + INTERVAL '5' SECOND
		WHERE
			t13e.epid IS NOT NULL
		GROUP BY 1,2,3,4,5
		HAVING mweb_guid1 = mweb_guid2 AND mweb_sskey1 = mweb_sskey2
	) vi
SET
	t2.dlk_brguid = vi.mweb_guid1,
	t2.dlk_brsess = vi.mweb_sskey1,
	t2.dlk_mweb_link_type = 5
WHERE
	vi.dlk_guid = t2.guid AND vi.dlk_sskey = t2.session_skey AND vi.site_id = t2.site_id AND dlk_brguid IS NULL;

UPDATE vi
FROM
	p_soj_cl_t.temp_csess13a_v9 vi,
	(
		SELECT
			vi.guid,
			vi.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			vi.item_id,
			vi.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY vi.guid, vi.session_skey, vi.item_id, vi.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13a_v9 vi
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON vi.user_id = dlk.user_id AND vi.item_id = dlk.dlk_itm AND dlk.event_timestamp BETWEEN vi.event_timestamp - INTERVAL '5' SECOND AND vi.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	vi.app_match_guid = dlk.dlk_guid,
	vi.app_match_sskey = dlk.dlk_sskey,
	vi.app_match_type = 1
WHERE
	vi.guid = dlk.guid AND vi.session_skey = dlk.session_skey AND vi.event_timestamp = dlk.event_timestamp AND vi.item_id = dlk.item_id AND vi.app_match_guid IS NULL;

UPDATE vi
FROM
	p_soj_cl_t.temp_csess13a_v9 vi,
	(
		SELECT
			vi.guid,
			vi.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			vi.item_id,
			vi.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY vi.guid, vi.session_skey, vi.item_id, vi.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13a_v9 vi
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON vi.item_id = dlk.dlk_itm AND vi.page_id = dlk.external_ip AND dlk.event_timestamp BETWEEN vi.event_timestamp - INTERVAL '5' SECOND AND vi.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	vi.app_match_guid = dlk.dlk_guid,
	vi.app_match_sskey = dlk.dlk_sskey,
	vi.app_match_type = 1
WHERE
	vi.guid = dlk.guid AND vi.session_skey = dlk.session_skey AND vi.event_timestamp = dlk.event_timestamp AND vi.item_id = dlk.item_id AND vi.app_match_guid IS NULL;

UPDATE vi
FROM
	p_soj_cl_t.temp_csess13a_v9 vi,
	(
		SELECT
			vi.guid,
			vi.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			vi.epid,
			vi.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY vi.guid, vi.session_skey, vi.epid, vi.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13a_v9 vi
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON vi.user_id = dlk.user_id AND vi.epid = dlk.dlk_epid AND dlk.event_timestamp BETWEEN vi.event_timestamp - INTERVAL '5' SECOND AND vi.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	vi.app_match_guid = dlk.dlk_guid,
	vi.app_match_sskey = dlk.dlk_sskey,
	vi.app_match_type = 2 /*epid*/
WHERE
	vi.guid = dlk.guid AND vi.session_skey = dlk.session_skey AND vi.event_timestamp = dlk.event_timestamp AND vi.app_match_guid IS NULL;

UPDATE vi
FROM
	p_soj_cl_t.temp_csess13a_v9 vi,
	(
		SELECT
			vi.guid,
			vi.session_skey,
			dlk.guid AS dlk_guid,
			dlk.session_skey AS dlk_sskey,
			dlk.user_id,
			vi.item_id,
			vi.event_timestamp,
			dlk.event_timestamp AS dlk_event_timestamp,
			SUM(1) OVER(PARTITION BY vi.guid, vi.session_skey, vi.epid, vi.event_timestamp ORDER BY dlk.event_timestamp ASC ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13a_v9 vi
			INNER JOIN p_soj_cl_t.temp_csess12aa_v9 dlk
			ON vi.epid = dlk.dlk_epid AND vi.page_id = dlk.external_ip AND dlk.event_timestamp BETWEEN vi.event_timestamp - INTERVAL '5' SECOND AND vi.event_timestamp + INTERVAL '15' SECOND
		QUALIFY first_rank = 1
	) dlk
SET
	vi.app_match_guid = dlk.dlk_guid,
	vi.app_match_sskey = dlk.dlk_sskey,
	vi.app_match_type = 2 /*epid*/
WHERE
	vi.guid = dlk.guid AND vi.session_skey = dlk.session_skey AND vi.event_timestamp = dlk.event_timestamp AND vi.app_match_guid IS NULL;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			vi.guid,
			vi.session_skey,
			vi.session_start_dt,
			vi.site_id,
			vi.cobrand,
			vi.primary_app_id,
			vi.event_timestamp,
			vi.app_match_guid,
			vi.app_match_sskey,
			vi.app_match_type,
			SUM(1) OVER(PARTITION BY vi.guid, vi.session_skey, vi.session_start_dt, vi.site_id, vi.cobrand ORDER BY vi.event_timestamp ROWS Unbounded PRECEDING) AS first_rank
		FROM
			p_soj_cl_t.temp_csess13a_v9 vi
		WHERE
			vi.app_match_guid IS NOT NULL
		QUALIFY first_rank = 1
	) vi
SET
	t2.session_details = COALESCE(session_details, '') || '&appguid=' || CAST(vi.app_match_guid AS STRING) || '&appsess=' || CAST(vi.app_match_sskey AS STRING) || '&applink=' || CAST(vi.app_match_type AS STRING)
WHERE
	vi.guid = t2.guid AND vi.session_skey = t2.session_skey AND vi.session_start_dt = t2.session_start_dt AND vi.site_id = t2.site_id AND vi.cobrand = t2.cobrand AND sojlib.soj_nvl(t2.session_details,'appguid') IS NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;