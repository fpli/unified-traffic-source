DROP TABLE IF EXISTS p_soj_cl_t.temp_csess8b_v9;
CREATE TABLE p_soj_cl_t.temp_csess8b_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	MAX(
		CASE
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE '%Android%'
				THEN 2571
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE '%iPad%'
				THEN 2878
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE ANY ('%iPhone%', '%iPod%')
				THEN 1462
			ELSE NULL
		END) AS app_id
FROM
	ubi_v.ubi_event e
WHERE
	e.session_start_dt = '2024-06-01'
	AND e.page_id = 5993
	AND e.url_query_string LIKE ANY ('%srcAppId=%', '%trackingApp=%', '%trackingNativeAppGuid=%')
GROUP BY 1,2,3
HAVING app_id IS NOT NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess8b2_v9;
CREATE TABLE p_soj_cl_t.temp_csess8b2_v9 USING delta AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	MAX(
		CASE
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE '%Android%'
				THEN 2571
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE '%iPad%'
				THEN 2878
			WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE ANY ('%iPhone%', '%iPod%')
				THEN 1462
			ELSE NULL
		END) AS app_id
FROM
	p_soj_cl_t.temp_csess1bot_v9 e
WHERE
	e.session_start_dt = '2024-06-01'
	AND e.page_id = 5993
	AND e.url_query_string LIKE ANY ('%srcAppId=%', '%trackingApp=%', '%trackingNativeAppGuid=%')
GROUP BY 1,2,3
HAVING app_id IS NOT NULL;

DELETE t8b2
FROM
	p_soj_cl_t.temp_csess8b_v9 t8b,
	p_soj_cl_t.temp_csess8b2_v9 t8b2
WHERE
	t8b.guid = t8b2.guid AND t8b.session_skey = t8b2.session_skey;

INSERT INTO p_soj_cl_t.temp_csess8b_v9 SELECT * FROM p_soj_cl_t.temp_csess8b2_v9;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess8b_v9 t8b,
	p_soj_cl_t.temp_csess2a_v9 t2a
SET
	t2a.primary_app_id = t8b.app_id,
	t2a.cobrand = 6,
	t2a.session_details = COALESCE(t2a.session_details, '') || '&sso=1'
WHERE
	t2a.guid = t8b.guid AND t2a.session_skey = t8b.session_skey AND t2a.session_start_dt = t8b.session_start_dt
	AND t2a.cobrand IN (6, 7)
	AND t2a.primary_app_id not in (35023, 35024)
	AND COALESCE(t2a.primary_app_id, -99) <> t8b.app_id;