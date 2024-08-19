DROP TABLE IF EXISTS p_soj_cl_t.temp_csess16a_v9;
CREATE TABLE p_soj_cl_t.temp_csess16a_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'bot_provider'), '%') AS bot_provider,
	0 AS ubi_bot,
	COUNT(*) AS events
FROM
	ubi_v.ubi_event e
WHERE
	e.session_start_dt = '2024-06-01' AND sojlib.soj_nvl(e.soj, 'bot_provider') IS NOT NULL
GROUP BY 1,2,3,4,5;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess16a2_v9;
CREATE TABLE p_soj_cl_t.temp_csess16a2_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	e.session_start_dt,
	sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'bot_provider'), '%') AS bot_provider,
	1 AS ubi_bot,
	COUNT(*) AS events
FROM
	p_soj_cl_t.temp_csess1bot_v9 e
WHERE
	e.session_start_dt = '2024-06-01' AND sojlib.soj_nvl(e.soj, 'bot_provider') IS NOT NULL
GROUP BY 1,2,3,4,5;

INSERT INTO p_soj_cl_t.temp_csess16a_v9 SELECT * FROM p_soj_cl_t.temp_csess16a2_v9;

DELETE FROM p_soj_cl_t.bot_provider_session_summ WHERE session_start_dt = '2024-06-01';

INSERT INTO p_soj_cl_t.bot_provider_session_summ
	(guid, session_skey, session_start_dt, site_id, cobrand, ubi_bot_flag, source, bot_provider, events)
SELECT t1.guid, t1.session_skey, t1.session_start_dt, t1.site_id, t1.cobrand, e.ubi_bot, 'AK' AS source, get_json_object(bot_provider, '$.providers.AK') AS bot_provider, sum(e.events) AS EVENTS
FROM
	p_soj_cl_t.temp_csess2a_v9 t1
	INNER JOIN p_soj_cl_t.temp_csess16a_v9 e
	ON t1.guid = e.guid AND t1.session_skey = e.session_skey AND t1.session_start_dt = e.session_start_dt
WHERE t1.session_start_dt = '2024-06-01' AND get_json_object(e.bot_provider, '$.providers.AK') IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8
UNION ALL
SELECT
	t1.guid, t1.session_skey, t1.session_start_dt, t1.site_id, t1.cobrand, e.ubi_bot, 'RW' AS source, get_json_object(bot_provider, '$.providers.RW') AS bot_provider, sum(e.events) AS EVENTS
FROM
	p_soj_cl_t.temp_csess2a_v9 t1
	INNER JOIN p_soj_cl_t.temp_csess16a_v9 e
	ON t1.guid = e.guid AND t1.session_skey = e.session_skey AND t1.session_start_dt = e.session_start_dt
WHERE t1.session_start_dt = '2024-06-01' AND get_json_object(bot_provider, '$.providers.RW') IS NOT NULL AND coalesce(get_json_object(bot_provider, '$.providers.RW.headers.rbc'),'-2') <> '-2'
GROUP BY 1,2,3,4,5,6,7,8
UNION ALL
SELECT
	t1.guid, t1.session_skey, t1.session_start_dt, t1.site_id, t1.cobrand, e.ubi_bot, 'EBAY' AS source, get_json_object(bot_provider, '$.providers.EBAY') AS bot_provider, sum(e.events) AS EVENTS
FROM
	p_soj_cl_t.temp_csess2a_v9 t1
	INNER JOIN p_soj_cl_t.temp_csess16a_v9 e
	ON t1.guid = e.guid AND t1.session_skey = e.session_skey AND t1.session_start_dt = e.session_start_dt
WHERE t1.session_start_dt = '2024-06-01' AND get_json_object(bot_provider, '$.providers.EBAY') IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8
UNION ALL
SELECT
	t1.guid, t1.session_skey, t1.session_start_dt, t1.site_id, t1.cobrand, e.ubi_bot, 'DT' AS source, get_json_object(bot_provider, '$.providers.DT') AS bot_provider, sum(e.events) AS EVENTS
FROM
	p_soj_cl_t.temp_csess2a_v9 t1
	INNER JOIN p_soj_cl_t.temp_csess16a_v9 e
	ON t1.guid = e.guid AND t1.session_skey = e.session_skey AND t1.session_start_dt = e.session_start_dt
WHERE
	t1.session_start_dt = '2024-06-01' AND get_json_object(bot_provider, '$.providers.DT') IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			guid,
			session_skey,
			session_start_dt
		FROM
			p_soj_cl_t.bot_provider_session_summ
		WHERE
			session_start_dt BETWEEN '2024-05-31' AND '2024-06-01' AND source = 'EBAY' AND get_json_object(bot_provider, '$.captcha.token') IS NOT NULL
		GROUP BY 1,2,3
	) c
SET
	t2.session_details = COALESCE(t2.session_details, '') || '&captcha=1'
WHERE
	c.guid = t2.guid AND c.session_skey = t2.session_skey AND c.session_start_dt = t2.session_start_dt AND sojlib.soj_nvl(t2.session_details,'captcha') IS NULL;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess16c_v9;
CREATE TABLE p_soj_cl_t.temp_csess16c_v9 USING PARQUET AS
SELECT
	t.guid,
	t.session_skey,
	t.session_start_dt,
	SUM(CASE WHEN source = 'AK' and replace(sojlib.soj_str_between_endlist(t.bot_provider, 'monitor:', '"}'), '+', ' ') LIKE ANY ('Request Anomaly', 'HTTP Libraries', 'Scraper Reputation', 'Declared Bots', 'Impersonators of Known Bots', 'Site Monitoring and Web Development Bots', 'RSS Feed Reader Bots', 'SynAck (Bug-Bounty)', 'Registration', 'RSS Feed Reader Bots', 'Web Search Engine Bots', 'Online Advertising Bots', 'Headless Browsers/Automation Tools', 'Open Source Crawlers/Scraping Platforms', 'Development Frameworks', 'Social Media or Blog Bots', 'SEO, Analytics or Marketing Bots', 'Enterprise Data Aggregator Bots', 'Web Services Libraries', 'E-Commerce Search Engine Bots', 'Web Archiver Bots', 'Aggressive Web Crawlers', 'News Aggregator Bots', 'Academic or Research Bots') THEN 1 ELSE 0 END) as ak_bot1,
	SUM(CASE WHEN sojlib.is_integer(sojlib.soj_extract_nvp(get_json_object(t.bot_provider, '$.headers.akr'),'WEBATCK',';','=')) = 1 AND sojlib.soj_extract_nvp(get_json_object(t.bot_provider, '$.headers.akr'),'WEBATCK',';','=') >=9 THEN 1 ELSE 0 END) as ak_bot2,
	SUM(CASE WHEN sojlib.is_integer(sojlib.soj_extract_nvp(get_json_object(t.bot_provider, '$.headers.akr'),'WEBSCRP',';','=')) = 1 AND sojlib.soj_extract_nvp(get_json_object(t.bot_provider, '$.headers.akr'),'WEBSCRP',';','=') >=9 THEN 1 ELSE 0 END) as ak_bot3,
	SUM(CASE WHEN source = 'RW' AND get_json_object(bot_provider, '$.headers.rbc') IN (17,21) THEN 1 WHEN source = 'RW' AND get_json_object(bot_provider, '$.headers.rbc') IN (22) AND get_json_object(bot_provider, '$.headers.rba') IN (2) THEN 1 ELSE 0 END) AS rw_bot,
	SUM(CASE WHEN sojlib.is_bigint(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}')) <> 1 THEN 0 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(1 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(8 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(16 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(512 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(1024 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(4096 AS BIGINT) > 0 THEN 1 WHEN CAST(sojlib.soj_str_between_endlist(t.bot_provider, '"xdb":"', '"}') AS BIGINT) & CAST(2097152 AS BIGINT) > 0 THEN 1 ELSE 0 END) as dst_bot,
	SUM(CASE WHEN source = 'EBAY' AND get_json_object(t.bot_provider, '$.captcha.token') IS NOT NULL THEN 1 ELSE 0 END) AS captcha_token
FROM
	p_soj_cl_t.bot_provider_session_summ t
WHERE
    session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
GROUP BY 1,2,3
HAVING (ak_bot1 + ak_bot2 + ak_bot3 + rw_bot + dst_bot) > 0 and captcha_token = 0;