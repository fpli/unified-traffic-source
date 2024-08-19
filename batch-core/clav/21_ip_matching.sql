DROP TABLE IF EXISTS p_soj_cl_t.temp_csess18a_v9;
CREATE TABLE p_soj_cl_t.temp_csess18a_v9 USING PARQUET AS
SELECT
	s.guid AS mweb_guid,
	s.session_skey AS mweb_sskey,
	s.session_start_dt AS mweb_sess_start_dt,
	s.site_id AS mweb_site_id,
	s.cobrand AS mweb_cobrand,
	s.primary_app_id AS mweb_app_id,
	COALESCE(signedin_user_id, mapped_user_id) AS mweb_uid,
	CAST(CASE WHEN app_id IN (1462,2878) AND sojlib.soj_nvl(s.agent_details, 'ost') LIKE '%iOS%' THEN 1 WHEN app_id IN (2571) AND sojlib.soj_nvl(s.agent_details, 'ost') LIKE '%Android%' THEN 1 ELSE 0 END AS TINYINT) AS ost_match,
	d.guid AS dlk_guid,
	d.session_skey AS dlk_sskey,
	d.site_id AS dlk_site_id,
	d.app_id AS dlk_app_id,
	d.event_timestamp AS dlk_ts,
	d.deeplink,
	d.dl_type
FROM
	p_soj_cl_t.temp_csess2a_v9 s /*mweb sessions*/
	INNER JOIN p_soj_cl_t.temp_csess12aa_v9 d /*deeplinks*/
	ON (s.ip = d.external_ip AND d.event_timestamp BETWEEN s.start_timestamp - INTERVAL '15' SECOND AND s.end_timestamp + INTERVAL '15' SECOND)
WHERE
	s.cobrand IN (6,7) AND COALESCE(primary_app_id, 3564) = 3564 AND s.session_details NOT LIKE '%appguid%' /* not linked */
	AND CAST(CASE WHEN app_id IN (1462,2878) AND sojlib.soj_nvl(s.agent_details, 'ost') LIKE '%iOS%' THEN 1 WHEN app_id IN (2571) AND sojlib.soj_nvl(s.agent_details, 'ost') LIKE '%Android%' THEN 1 ELSE 0 END AS TINYINT) = 1;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess18b_v9;
CREATE TABLE p_soj_cl_t.temp_csess18b_v9 USING PARQUET AS
SELECT
	ip.mweb_guid,
	ip.mweb_sskey,
	ip.mweb_sess_start_dt,
	ip.mweb_site_id,
	ip.mweb_cobrand,
	ip.mweb_app_id,
	ip.mweb_uid,
	p2.page_name AS mweb_page_name,
	p.page_fmly4_name AS mweb_page_fmly4_name,
	p.event_timestamp AS mweb_evt_ts,
	ip.dlk_guid,
	ip.dlk_sskey,
	ip.dlk_site_id,
	ip.dlk_app_id,
	ip.dlk_ts,
	COALESCE(d.signedin_user_id, d.mapped_user_id) AS dlk_uid,
	CASE
		WHEN deeplink IS NULL
			THEN 'unknown'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/itm/%'
			THEN 'itm'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/i/%'
			THEN 'PRP (item-based)'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/p/%'
			THEN 'PRP'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/e/%'
			THEN 'events'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/b/%'
			THEN 'browse'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/sch/%'
			THEN 'search'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE ANY ('%/payments.ebay%','%/payments.cafr.ebay%')
			THEN 'xo'
		WHEN sojlib.soj_get_url_path(deeplink) LIKE '%/res.ebay%'
			THEN 'trust'
		WHEN deeplink LIKE ANY ('ebay://item/VIEW?id=%','%?nav=item.view%','%?itm=%','%?nav=item.view%','%?nav=item.view%','http://cgi.ebay.%/ws/eBayISAPI.dll?ViewItem%')
			THEN 'itm'
		WHEN deeplink LIKE ANY ('%?nav=user.xo%','https://cart.ebay%','http://payments.%')
			THEN 'xo'
		WHEN deeplink LIKE '%?nav=user.compose%'
			THEN 'compose'
		WHEN deeplink LIKE '%?nav=user.bid%'
			THEN 'bid'
		WHEN deeplink LIKE '%?nav=user.review%'
			THEN 'writereview'
		WHEN deeplink LIKE '%?nav=user.sell%'
			THEN 'sell'
		WHEN deeplink LIKE '%?nav=user.cart%'
			THEN 'cart'
		WHEN deeplink LIKE '%?nav=user.coupon%'
			THEN 'coupon'
		WHEN deeplink LIKE '%?nav=user.signin%'
			THEN 'singin'
		WHEN deeplink LIKE '%?nav=user.contactseller%'
			THEN 'contactseller'
		WHEN deeplink LIKE '%?nav=user.myebay%'
			THEN 'myebay'
		WHEN deeplink LIKE '%?nav=user.saved_searches%'
			THEN 'savedsch'
		WHEN deeplink LIKE '%?nav=2fa.%'
			THEN '2fa'
		WHEN deeplink LIKE '%?nav=item.browse%'
			THEN 'browse'
		WHEN deeplink LIKE '%?nav=item.query%'
			THEN 'search'
		WHEN deeplink LIKE '%?nav=user.view%'
			THEN 'profile'
		WHEN deeplink LIKE '%?nav=item.product%'
			THEN 'PRP (item-based)'
		WHEN deeplink LIKE '%?nav=prp%'
			THEN 'PRP'
		WHEN deeplink LIKE ANY ('%?nav=item.deals%','http://deals.ebay.com/%')
			THEN 'deals'
		WHEN deeplink LIKE ANY ('https://www.ebay.ca/e/sales-events/%','%?nav=item.events%')
			THEN 'events'
		WHEN deeplink LIKE '%?nav=home%'
			THEN 'home'
		WHEN deeplink LIKE '%?nav=webview%'
			THEN 'contactcs'
		WHEN deeplink LIKE '%?nav=user.offerfromseller%'
			THEN 'sio'
		WHEN deeplink LIKE 'http://contact.ebay.%/ws/eBayISAPI.dll?M2MContact%'
			THEN 'm2m'
		WHEN deeplink LIKE 'http://mesgmy.ebay.%/ws/eBayISAPI.dll?ViewMyMessageDetails%'
			THEN 'viewmsgs'
		WHEN deeplink LIKE 'https://signin.ebay%'
			THEN 'signin'
		WHEN deeplink LIKE 'http://res.ebay%'
			THEN 'trust'
		WHEN deeplink LIKE ANY ('http://m.ebay.%/myebay','http://m.ebay.%/myebay?')
			THEN 'myebay'
		WHEN deeplink LIKE ANY ('ebay://launch/?nav=user.priceGuidanceRevise%')
			THEN 'myebay.prguide'
		WHEN deeplink LIKE ANY ('ebay:','ebay://')
			THEN 'home'
		WHEN deeplink LIKE ANY ('http://pages.ebay.com/help/account/recognizing-spoof.html?%')
			THEN 'spoof'
		WHEN deeplink LIKE ANY ('http://pages.ebay.%/help/%','https://www.ebay.co.uk/HELP/home')
			THEN 'help'
		ELSE 'other'
	END AS dlk_target,
	ip.dl_type,
	ip.deeplink,
	SUM(1) Over(PARTITION BY ip.mweb_guid, ip.mweb_sskey, ip.mweb_site_id, ip.dlk_guid, ip.dlk_sskey, ip.dlk_site_id ORDER BY ip.dlk_ts, p.event_timestamp ROWS Unbounded Preceding) AS dedupe_rank
FROM
	p_soj_cl_t.temp_csess18a_v9 ip
	INNER JOIN p_soj_cl_t.temp_csess1c_v9 p
	ON p.guid = ip.mweb_guid AND p.session_skey = ip.mweb_sskey AND ip.dlk_ts BETWEEN p.event_timestamp - INTERVAL '5' SECOND AND p.event_timestamp + INTERVAL '5' SECOND
	INNER JOIN p_soj_cl_v.pages p2
	ON p2.page_id = p.page_id
	INNER JOIN p_soj_cl_t.temp_csess2a_v9 d /*deeplinked session*/
	ON d.guid = ip.dlk_guid AND d.session_skey = ip.dlk_sskey AND d.cobrand IN (6,7)
WHERE
	ip.dl_type NOT IN ('nactn','nhubactn')
	AND COALESCE(ip.mweb_uid,COALESCE(d.signedin_user_id,d.mapped_user_id),9999) = COALESCE(COALESCE(d.signedin_user_id,d.mapped_user_id),ip.mweb_uid,9999)
QUALIFY dedupe_rank = 1;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t18b.mweb_guid,
			t18b.mweb_sskey,
			t18b.mweb_sess_start_dt,
			t18b.mweb_site_id,
			t18b.dlk_guid AS app_match_guid,
			t18b.dlk_sskey AS app_match_sskey,
			6 AS app_match_type,
			t18b.dlk_ts,
			SUM(1) Over(PARTITION BY t18b.mweb_guid,t18b.mweb_sskey,t18b.mweb_sess_start_dt,t18b.mweb_site_id ORDER BY t18b.dlk_ts ASC ROWS Unbounded Preceding) AS rank1
		FROM
			p_soj_cl_t.temp_csess18b_v9 t18b
		QUALIFY rank1 = 1
	) t18b
SET
	t2.session_details = COALESCE(session_details, '') || '&appguid=' || CAST(t18b.app_match_guid AS STRING) || '&appsess=' || CAST(t18b.app_match_sskey AS STRING) || '&applink=' || CAST(t18b.app_match_type AS STRING)
WHERE
	t18b.mweb_guid = t2.guid AND t18b.mweb_sskey = t2.session_skey AND t18b.mweb_sess_start_dt = t2.session_start_dt AND t18b.mweb_site_id = t2.site_id AND sojlib.soj_nvl(t2.session_details,'appguid') IS NULL AND t2.cobrand IN (6,7);

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			dlk.guid,
			dlk.session_skey,
			dlk.session_start_dt,
			dlk.site_id,
			dlk.cobrand,
			dlk.primary_app_id,
			dlk.dlk_entry_ts,
			ip.dlk_ts,
			ip.mweb_guid,
			ip.mweb_sskey,
			ip.mweb_site_id,
			ip.mweb_evt_ts,
			SUM(1) Over(PARTITION BY dlk.guid, dlk.session_skey ORDER BY ip.mweb_evt_ts ROWS Unbounded Preceding) AS fst_rank
		FROM
			p_soj_cl_t.temp_csess18b_v9 ip
			INNER JOIN p_soj_cl_t.temp_csess2a_v9 dlk /* app session */
			ON dlk.guid = ip.dlk_guid AND dlk.session_skey = ip.dlk_sskey AND dlk.site_id = ip.dlk_site_id
		WHERE dlk_entry_ts IS NOT NULL AND dlk_brguid IS NULL AND ip.mweb_evt_ts BETWEEN dlk.dlk_entry_ts - INTERVAL '10' SECOND AND dlk.dlk_entry_ts + INTERVAL '5' SECOND
		QUALIFY fst_rank = 1
	) d
SET
	t2.dlk_brguid = d.mweb_guid,
	t2.dlk_brsess = d.mweb_sskey,
	t2.dlk_mweb_link_type = 11 /*ip*/
WHERE
	d.guid = t2.guid AND d.session_skey = t2.session_skey AND d.site_id = t2.site_id AND d.session_start_dt = t2.session_start_dt AND t2.dlk_brguid IS NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;