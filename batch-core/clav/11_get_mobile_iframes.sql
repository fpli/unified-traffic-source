DROP TABLE IF EXISTS p_soj_cl_t.temp_csess8aa_v9;
CREATE TABLE p_soj_cl_t.temp_csess8aa_v9 USING delta AS
SELECT
	e.guid,
	e.session_skey,
	6 AS cobrand,
	CAST(
		CASE
			WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1
				THEN sojlib.soj_nvl(e.soj, 'app')
			ELSE NULL
		END AS INTEGER) AS app,
	e.session_start_dt,
	CAST(NULL AS DECIMAL(4,0)) AS user_cntry_id,
	CAST(NULL AS STRING) AS device_type,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'n') AS STRING)) AS cguid,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'bs')) = 1 THEN sojlib.soj_nvl(e.soj, 'bs') ELSE NULL END AS INTEGER)) AS buyer_site_id,
	MIN(CAST(replace(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'carrier'), '%'), '%') , '+', ' ') AS STRING)) AS carrier,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'dn') AS STRING)) AS device_name,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'ec')) = 1 THEN sojlib.soj_nvl(e.soj, 'ec') ELSE NULL END AS INTEGER)) AS ec,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'es')) = 1 THEN sojlib.soj_nvl(e.soj, 'es') ELSE NULL END AS INTEGER)) AS es,
	MIN(CAST(SUBSTR(sojlib.soj_nvl(e.soj, 'idfa'),1,36) AS STRING)) AS idfa,
	MAX(CAST(sojlib.soj_nvl(e.soj, 'mav') AS STRING)) AS mav,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'maup') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'maup') ELSE NULL END AS TINYINT)) AS maup,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'mdnd') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'mdnd') ELSE NULL END AS TINYINT)) AS mdnd,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'mlch') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'mlch') ELSE NULL END AS TINYINT)) AS mlch,
	MIN(CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'mloc'), '%') AS STRING)) AS mloc,
	MIN(CAST(LOWER(sojlib.soj_nvl(e.soj, 'mnt')) AS STRING)) AS mnt,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'mos') AS STRING)) AS mos,
	MIN(CASE WHEN sojlib.is_bigint(sojlib.soj_nvl(e.soj, 'mppid')) = 1 THEN CAST(sojlib.soj_nvl(e.soj, 'mppid') AS BIGINT) ELSE NULL END) AS mppid,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'ort') AS STRING)) AS ort,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'osv') AS STRING)) AS osv,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'prefl') AS STRING)) AS prefl,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'res') AS STRING)) AS res,
	MIN(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'rlutype')) = 1 THEN sojlib.soj_nvl(e.soj, 'rlutype') ELSE NULL END AS TINYINT)) AS rlutype,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'uc') AS STRING)) AS uc_tag,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'ul') AS STRING)) AS ul,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'user_name') AS STRING)) AS user_name,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'gadid') AS STRING)) AS gadid,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'theme') AS STRING)) AS theme,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'formFactor') AS STRING)) AS formFactor,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'tablet') AS STRING)) AS tablet,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'mobile') AS STRING)) AS mobile,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'dsktop') AS STRING)) AS dsktop,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'ist')) = 1 THEN sojlib.soj_nvl(e.soj, 'ist') ELSE NULL END AS TINYINT)) AS ist,
	MIN(CAST(CASE WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'mrollp'), 6, 2) = 1 THEN sojlib.soj_nvl(e.soj, 'mrollp') ELSE NULL END AS DECIMAL(6,2))) AS mrollp,
	MAX(CASE WHEN sojlib.soj_nvl(e.soj, 'awat') = '1' THEN 1 ELSE 0 END) AS awat,
	MAX(CASE WHEN e.page_id IN (2050494,1673581,1698105,2034596,2054180,2051248,2050605,2050535) THEN 1 ELSE 0 END) AS fg,
	MAX(CASE WHEN e.page_id IN (2050495,2051249,2050606) THEN 1 ELSE 0 END) AS bg,
	MAX(CAST(
				CASE
					WHEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') IS NOT NULL AND sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP')
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1)
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1)
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2)
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2)
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3)
					WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
						THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3)
					ELSE NULL
				END AS STRING)) AS ext_ip
FROM
	ubi_v.ubi_event e
WHERE
	e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND e.page_id IN (2356359,2367320,2054121,2050465,2047936,2062300,2051457,2054060,2065434,2054081,2065432,2051248,2051249,2050535,2057087,2054081,2059087,2056372,2052310,2054060,2053277,2058946,2054180,2050494,2050495,2058483,2050605,2050606,1673581,1698105,2034596,2041594,1677709)
	AND e.site_id IS NOT NULL
	AND sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1
GROUP BY 1,2,3,4,5;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess8a2_v9;
CREATE TABLE p_soj_cl_t.temp_csess8a2_v9 USING delta AS
SELECT
	e.guid,
	e.session_skey,
	6 AS cobrand,
	CAST(
		CASE
			WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1
				THEN sojlib.soj_nvl(e.soj, 'app')
			ELSE NULL
		END AS INTEGER) AS app,
	e.session_start_dt,
	CAST(NULL AS DECIMAL(4,0)) AS user_cntry_id,
	CAST(NULL AS STRING) AS device_type,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'n') AS STRING)) AS cguid,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'bs')) = 1 THEN sojlib.soj_nvl(e.soj, 'bs') ELSE NULL END AS INTEGER)) AS buyer_site_id,
	MIN(CAST(replace(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'carrier'), '%'), '%') , '+', ' ') AS STRING)) AS carrier,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'dn') AS STRING)) AS device_name,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'ec')) = 1 THEN sojlib.soj_nvl(e.soj, 'ec') ELSE NULL END AS INTEGER)) AS ec,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'es')) = 1 THEN sojlib.soj_nvl(e.soj, 'es') ELSE NULL END AS INTEGER)) AS es,
	MIN(CAST(SUBSTR(sojlib.soj_nvl(e.soj, 'idfa'),1,36) AS STRING)) AS idfa,
	MAX(CAST(sojlib.soj_nvl(e.soj, 'mav') AS STRING)) AS mav,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'maup') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'maup') ELSE NULL END AS TINYINT)) AS maup,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'mdnd') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'mdnd') ELSE NULL END AS TINYINT)) AS mdnd,
	MAX(CAST(CASE WHEN sojlib.soj_nvl(e.soj, 'mlch') IN ('0', '1') THEN sojlib.soj_nvl(e.soj, 'mlch') ELSE NULL END AS TINYINT)) AS mlch,
	MIN(CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'mloc'), '%') AS STRING)) AS mloc,
	MIN(CAST(LOWER(sojlib.soj_nvl(e.soj, 'mnt')) AS STRING)) AS mnt,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'mos') AS STRING)) AS mos,
	MIN(CASE WHEN sojlib.is_bigint(sojlib.soj_nvl(e.soj, 'mppid')) = 1 THEN CAST(sojlib.soj_nvl(e.soj, 'mppid') AS BIGINT) ELSE NULL END) AS mppid,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'ort') AS STRING)) AS ort,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'osv') AS STRING)) AS osv,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'prefl') AS STRING)) AS prefl,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'res') AS STRING)) AS res,
	MIN(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'rlutype')) = 1 THEN sojlib.soj_nvl(e.soj, 'rlutype') ELSE NULL END AS TINYINT)) AS rlutype,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'uc') AS STRING)) AS uc_tag,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'ul') AS STRING)) AS ul,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'user_name') AS STRING)) AS user_name,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'gadid') AS STRING)) AS gadid,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'theme') AS STRING)) AS theme,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'formFactor') AS STRING)) AS formFactor,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'tablet') AS STRING)) AS tablet,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'mobile') AS STRING)) AS mobile,
	MIN(CAST(sojlib.soj_nvl(e.soj, 'dsktop') AS STRING)) AS dsktop,
	MAX(CAST(CASE WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'ist')) = 1 THEN sojlib.soj_nvl(e.soj, 'ist') ELSE NULL END AS TINYINT)) AS ist,
	MIN(CAST(CASE WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'mrollp'), 6, 2) = 1 THEN sojlib.soj_nvl(e.soj, 'mrollp') ELSE NULL END AS DECIMAL(6,2))) AS mrollp,
	MAX(CASE WHEN sojlib.soj_nvl(e.soj, 'awat') = '1' THEN 1 ELSE 0 END) AS awat,
	MAX(CASE WHEN e.page_id IN (2050494,1673581,1698105,2034596,2054180,2051248,2050605,2050535) THEN 1 ELSE 0 END) AS fg,
	MAX(CASE WHEN e.page_id IN (2050495,2051249,2050606) THEN 1 ELSE 0 END) AS bg,
	MAX(CAST(
			CASE
				WHEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') IS NOT NULL AND sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP')
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1)
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1)
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 2)
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 2)
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 3)
				WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3) IS NOT NULL AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3) NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 3)
				ELSE NULL
			END AS STRING)) AS ext_ip
FROM
	p_soj_cl_t.temp_csess1bot_v9 e
WHERE
	e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	AND e.page_id IN (2356359,2367320,2054121,2050465,2047936,2062300,2051457,2054060,2065434,2054081,2065432,2051248,2051249,2050535,2057087,2054081,2059087,2056372,2052310,2054060,2053277,2058946,2054180,2050494,2050495,2058483,2050605,2050606,1673581,1698105,2034596,2041594,1677709)
	AND e.site_id IS NOT NULL
	AND sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1
GROUP BY 1,2,3,4,5 ;

DELETE t8a2
FROM
	p_soj_cl_t.temp_csess8a2_v9 t8a2,
	p_soj_cl_t.temp_csess8aa_v9 t8a
WHERE
	t8a2.guid = t8a.guid AND t8a2.session_skey = t8a.session_skey AND t8a2.session_start_dt = t8a.session_start_dt;

INSERT INTO p_soj_cl_t.temp_csess8aa_v9 SELECT * FROM p_soj_cl_t.temp_csess8a2_v9;

UPDATE
    p_soj_cl_t.temp_csess8aa_v9
SET
    user_cntry_id = CAST(uc_tag AS INTEGER)
WHERE
    sojlib.is_integer(uc_tag) = 1;

UPDATE a
FROM
	p_soj_cl_t.temp_csess8aa_v9 a,
	(
		SELECT
			device_name,
			SUM(tablet_recs) AS tblt,
			SUM(phone_recs) AS phn,
			CASE
				WHEN (SUM(tablet_recs) * 1.5) > (SUM(tablet_recs) + phn)
					THEN 'Tablet'
				WHEN (phn * 1.5) > (SUM(tablet_recs) + phn)
					THEN 'Phone'
				ELSE 'Undetermined'
			END AS classification
		FROM
			p_soj_cl_t.dev_hist1
		WHERE
			device_name IS NOT NULL
		GROUP BY 1
		HAVING classification <> 'Undetermined'
	) dt
SET
	a.device_type = dt.classification
WHERE
	sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(a.device_name, '%'), '%') = dt.device_name AND a.device_type IS NULL AND dt.classification IS NOT NULL ;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess8aa_v9 t8a,
	p_soj_cl_t.temp_csess2a_v9 t2a
SET
	t2a.cguid = CASE WHEN t2a.cguid IS NULL THEN t8a.cguid ELSE t2a.cguid END,
	t2a.session_details = CAST(COALESCE(t2a.session_details, '') || CAST( CASE WHEN t8a.mav IS NOT NULL THEN '&mav=' || t8a.mav ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.osv IS NOT NULL THEN '&osv=' || t8a.osv ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mos IS NOT NULL THEN '&mos=' || t8a.mos ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.buyer_site_id IS NOT NULL THEN '&bs=' || CAST(t8a.buyer_site_id AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.idfa IS NOT NULL THEN '&idfa=' || t8a.idfa ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.maup IS NOT NULL THEN '&maup=' || CAST(t8a.maup AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mdnd IS NOT NULL THEN '&mdnd=' || CAST(t8a.mdnd AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mlch IS NOT NULL THEN '&mlch=' || CAST(t8a.mlch AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.gadid IS NOT NULL THEN '&gadid=' || t8a.gadid ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mrollp IS NOT NULL THEN '&mrollp=' || CAST(t8a.mrollp AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.device_name IS NOT NULL THEN '&dn=' || t8a.device_name ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mppid IS NOT NULL AND sojlib.soj_nvl(t2a.session_details, 'mppid') IS NULL THEN '&mppid=' || CAST(t8a.mppid AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.user_name IS NOT NULL THEN '&user_name=' || t8a.user_name ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mnt IS NOT NULL AND sojlib.soj_nvl(t2a.session_details, 'mnt') IS NULL THEN '&mnt=' || t8a.mnt ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mloc IS NOT NULL THEN '&mloc=' || t8a.mloc ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.ul IS NOT NULL THEN '&ul=' || t8a.ul ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.ist IS NOT NULL THEN '&ist=' || CAST(t8a.ist AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.formFactor IS NOT NULL THEN '&formfactor=' || CAST(t8a.formFactor AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.tablet IS NOT NULL THEN '&tablet=' || CAST(t8a.tablet AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.mobile IS NOT NULL THEN '&mobile=' || CAST(t8a.mobile AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.dsktop IS NOT NULL THEN '&dsktop=' || CAST(t8a.dsktop AS STRING) ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.res IS NOT NULL THEN '&res=' || t8a.res ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.carrier IS NOT NULL THEN '&carrier=' || replace(replace(replace(t8a.carrier, '%', '%25'), '&', '%26'), '=', '%3D') ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.awat IS NOT NULL THEN '&awat=' || t8a.awat ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.fg = 1 THEN '&fg=1' ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.bg = 1 THEN '&bg=1' ELSE '' END AS STRING)
							|| CAST(CASE WHEN t8a.theme IS NOT NULL AND t8a.theme NOT IN ('undefined') THEN '&theme=' || t8a.theme ELSE '' END AS STRING) AS STRING),
	t2a.device_type = CASE
						WHEN t8a.awat = 1
							THEN 'Smartwatch'
						WHEN t8a.device_type IS NOT NULL AND t8a.device_type <> ''
							THEN t8a.device_type
						WHEN lower(t8a.device_name) LIKE 'ipad%'
							THEN 'Tablet'
						WHEN lower(t8a.device_name) LIKE 'ipod%'
							THEN 'MP3'
						WHEN lower(t8a.device_name) LIKE 'iphone%'
							THEN 'Phone'
						WHEN t8a.ist IS NOT NULL AND t8a.ist = 1
							THEN 'Tablet'
						WHEN t8a.ist IS NOT NULL AND t8a.ist = 0
							THEN 'Phone'
						WHEN t8a.tablet IS NOT NULL AND t8a.tablet = 'true'
							THEN 'Tablet'
						WHEN t8a.formFactor IS NOT NULL AND t8a.formFactor = 'phone'
							THEN 'Phone'
						WHEN t8a.formFactor IS NOT NULL AND t8a.formFactor = 'tablet'
							THEN 'Tablet'
						WHEN t8a.formFactor IS NOT NULL AND t8a.formFactor = 'dsktop'
							THEN 'PC'
						WHEN t8a.formFactor IS NOT NULL AND t8a.formFactor = 'mobile'
							THEN 'Phone'
						WHEN t8a.device_name = 'x86_64'
							THEN 'PC: Mobile Emulator'
						WHEN t2a.device_type IS NOT NULL
							THEN t2a.device_type
						ELSE NULL
					END,
	t2a.ip = CASE
				WHEN (t2a.ip LIKE ANY ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%') OR t2a.ip IS NULL) AND t8a.ext_ip NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
					THEN t8a.ext_ip
				ELSE t2a.ip
			 END
WHERE
	t2a.guid = t8a.guid AND t2a.session_skey = t8a.session_skey AND t2a.session_start_dt = t8a.session_start_dt AND t2a.primary_app_id = t8a.app AND t2a.cobrand = 6;

UPDATE
    p_soj_cl_t.temp_csess2a_v9
SET
	session_details = COALESCE(session_details, '') || '&dt=' || device_type
WHERE
    sojlib.soj_nvl(session_details, 'dt') IS NULL AND device_type IS NOT NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;
