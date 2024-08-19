SELECT 'Now waiting for outlier_sessions...';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1a2_v9;

-- Create a new table with the same schema as the original table
CREATE TABLE p_soj_cl_t.temp_csess1a2_v9 USING delta
SELECT
	ev.guid,
	ev.session_skey,
	CAST(
		CASE
			WHEN ev.session_start_dt BETWEEN '2018-05-23' AND '2018-05-24' AND CAST(sojlib.soj_nvl(ev.client_data, 'TPool') AS STRING) = 'r1rover' AND sojlib.soj_nvl(ev.soj,'es') IS NOT NULL AND sojlib.is_integer(sojlib.soj_nvl(ev.soj, 'es')) = 1 AND CAST(sojlib.soj_nvl(ev.soj,'es') AS SMALLINT) <> ev.site_id AND app_id IN (2878,1462,2571)
				THEN sojlib.soj_nvl(ev.soj,'es')
			WHEN ev.web_server = 'in.ebay.com'
				THEN 203
			ELSE ev.site_id
		END AS SMALLINT) AS site_id,
	ev.session_start_dt,
	ev.page_id,
	ev.event_timestamp,
	ev.seqnum,
	ev.rdt,
	ev.pn,
	CASE
		WHEN ev.app_id IN (1462,2878,2571,35023,35024)
			THEN ev.app_id
		ELSE NULL END AS app_id,
	CASE
		WHEN ev.rdt = 1 AND NOT (ev.page_id IN (2045573,2046732,2045576,2189159) AND ev.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
			THEN 0
		WHEN ev.page_id = 451 AND lower(ev.url_query_string) LIKE '%logbuyerregistrationjsevent%'
			THEN 0 /* REI JS logging */
		WHEN lower(ev.web_server) LIKE ANY ('%sandbox.ebay.%'/*, 'localhost'*/)
			THEN 0 /* Sandbox + QA pages */
		WHEN ev.page_id IN (2588,3030,3907,4939,5108,2050601)
			AND CASE
					WHEN sojlib.soj_nvl(ev.soj, 'cflgs') IS NOT NULL
						THEN sojlib.soj_extract_flag(sojlib.soj_nvl(ev.soj, 'cflgs'), 4)
					ELSE 0
				END = 1
			THEN 0 /* Firefox prefetches */
		WHEN
			CASE
				WHEN sojlib.soj_nvl(ev.soj, 'cflgs') IS NOT NULL
					THEN sojlib.soj_extract_flag(sojlib.soj_nvl(ev.soj, 'cflgs'), 14)
					ELSE 0
			END = 1
			THEN 0 /* Safari prefetches */
		WHEN lower(ev.sQr) LIKE ANY ('%.html', '%.asp', '%.jsp', '%.gif', '%.png', '%.pdf', '%.htm', '%.php', '%.cgi', '%.jpeg', '%.swf', '%.txt', '%.wav', '%.zip', '%.flv', '%.dll', '%.ico', '%.jpg', 'null', 'undefined', '%hideoutput%')
			THEN 0 /* Bad search queries */
		WHEN ev.page_id = 1468660 AND ev.site_id = 0 AND ev.session_start_dt >= '2010-08-19' AND ev.web_server = 'rover.ebay.com'
			THEN 0 /* Daily Deals page double-counting; switched to native server-side logging */
		WHEN ev.page_id IN (1702440,2043183,2043216,2051542,2051322,2051319,2052193,2051542,2052317,3693,2047675,2054574,2057587,2052197,2049334,2052122,2051865,4853,2504330,2504331,5410) AND ev.web_server LIKE 'rover.ebay.%'
			THEN 0 /* Many pages are double-counting; only rely on server-side logging */
		WHEN sojlib.soj_nvl(ev.soj, 'an') IS NOT NULL AND sojlib.soj_nvl(ev.soj, 'av') IS NOT NULL
			THEN 0 /* RoverImp click tracking */
		WHEN sojlib.soj_nvl(ev.soj, 'in') IS NOT NULL
			THEN 0 /* RoverImp AJAX click tracking */
		WHEN ev.page_id = 5360 AND lower(ev.url_query_string) LIKE '%_xhr=2%'
			THEN 0 /* Duplicate PDP requests */
		WHEN lower(ev.url_query_string) LIKE ANY ('/_vti_bin%', '/msoffice/cltreq.asp%')
			THEN 0 /* Weird extra events that are coming in from either Office integration or adware */
		WHEN sojlib.soj_nvl(ev.soj, 'mr') = '1' OR (lower(ev.url_query_string) LIKE ANY ('%\?redirect=mobile%', '%&redirect=mobile%') AND ev.page_id NOT IN (2255925, 2507978))
			THEN 0 /* Mobile redirects, added exception to handle false positives created by XO */
		WHEN ev.page_id = 2043141 AND lower(ev.url_query_string) LIKE '%jfs.js%'
			THEN 0 /* CSE Listing flow JS requests happening to same command as main page */
		WHEN ev.page_id IN (2765,2771,2685,3306,2769,4034,4026) AND sojlib.soj_nvl(ev.soj, 'state') IS NULL
			THEN 0 /* Many listing commands are also surfacing non-page events */
		WHEN lower(ev.url_query_string) LIKE '%/_showdiag=1%'
			THEN 0 /* ShowDiag events are for internal testing only. Often used by scrapers to grab search back-end queries. */
		WHEN sojlib.soj_parse_clientinfo(ev.client_data, 'RemoteIP') IN ('10.2.137.50', '10.2.182.150', '10.2.137.51', '10.2.182.151')
			THEN 0 /* Search replay scrapers */
		WHEN lower(ev.url_query_string) LIKE ANY ('/ ', '/&nbsb;')
			THEN 0 /* Malformed URLs */
		WHEN ev.page_id = 1677950 AND ev.sqr = 'postalCodeTestQuery'
			THEN 0 /* Odd Android app background event */
		WHEN ev.page_id IN (5713,2053584,6024,2053898,6053,2054900) AND (sojlib.soj_nvl(ev.soj, 'page') NOT IN ('ryprender', 'cyp', 'success', 'pots', 'error', 'rypexpress', 'MainCheckoutPage') OR sojlib.soj_nvl(ev.soj, 'page') IS NULL)
			THEN 0 /* XOProcessor extra events */
		WHEN sojlib.soj_parse_clientinfo(ev.client_data, 'Agent') LIKE ANY ('eBayNioHttpClient%') AND ev.page_id NOT IN (2050757)
			THEN 0 /* Pool-to-pool calls - exception for legit Fashion landing pages */
		WHEN ev.page_id IN (2050867,2052122,2050519) AND lower(ev.url_query_string) LIKE '%json%'
			THEN 0 /* JSON calls */
		WHEN ev.page_id IN (2050867) AND lower(ev.url_query_string) LIKE '/local/availability%'
			THEN 0 /* Local extra calls */
		WHEN lower(ev.url_query_string) LIKE ANY ('%.gif', '%.png', '%.pdf', '%.jpeg', '%.swf', '%.txt', '%.wav', '%.zip', '%.flv', '%.ico', '%.jpg', 'null', 'undefined')
			THEN 0 /* Extra image requests */
		WHEN ev.page_id = 2050601 AND lower(ev.page_name) NOT LIKE 'feedhome%'
			THEN 0 /* Home Page AJAX requests */
		WHEN ev.page_id = 2054095 AND lower(ev.url_query_string) NOT LIKE '/survey%'
			THEN 0 /* Survey POST requests */
		WHEN ev.page_id = 2056116 AND lower(ev.url_query_string) LIKE ANY ('/itm/watchinline%', '/itm/ajaxsmartappbanner%')
			THEN 0 /* mWeb Watch events masquerading as VI */
		WHEN ev.page_id = 2059707 AND lower(ev.url_query_string) LIKE '/itm/delivery%'
			THEN 0 /* Indian mWeb Shipping tab on VI that's reusing the VI page ID */
		WHEN ev.page_id = 2052197 AND lower(ev.url_query_string) LIKE '%importhubitemdescription%'
			THEN 0 /* Indian GEB Item Description on VI that's an iFrame */
		WHEN ev.page_id = 2052197 AND lower(ev.url_query_string) LIKE '%Importhubcreatelisting%'
			THEN 0 /* Indian GEB iFrame on VI that's an iFrame */
		WHEN ev.page_id IN (2047935,2053898) AND lower(ev.web_server) LIKE 'reco.ebay.%'
			THEN 0 /* Merch iFrames */
		WHEN ev.page_id IN (2067339) AND lower(ev.url_query_string) LIKE '/roverimp/0/0/9?%'
			THEN 0 /* Clean up pages that are double-logged with IMG tag and TrackingJS */
		WHEN ev.page_id IN (2053898) AND (sojlib.soj_nvl(ev.url_query_string, 'page') NOT IN ('MainCheckoutPage', 'CheckoutPaymentSuccess','CheckoutPayPalWeb','PaymentSent','CheckoutPaymentMethod','Autopay','CheckoutPayPalError1','CheckoutPaymentFailed') OR sojlib.soj_nvl(ev.soj, 'page') IS NULL)
			THEN 0 /* More checkout cleanup */
		WHEN ev.page_id IN (2056812) AND (sojlib.soj_nvl(ev.soj, 'page') NOT IN ('ryprender', 'cyprender' ) OR sojlib.soj_nvl(ev.soj, 'page') IS NULL)
			THEN 0 /* Mobile checkout editing */
		WHEN ev.page_id = 2056116 AND sojlib.soj_nvl(ev.soj, 'pfn') NOT IN ('VI')
			THEN 0 /* Mobile Web VI-sub pages */
		WHEN ev.page_id = 2351460 AND ev.session_start_dt >= '2021-01-27' AND sojlib.soj_nvl(ev.soj, 'sHit') IS NULL AND sojlib.soj_nvl(ev.soj, 'app') in (1462,2878)
			THEN 0 /*Experience Services Search */
		WHEN ev.page_id IN (2385738) AND lower(sojlib.soj_parse_clientinfo(ev.client_data, 'Agent')) LIKE ANY ('ebay%') AND (sojlib.soj_nvl(ev.soj, 'app') = '3564' OR sojlib.soj_nvl(ev.soj, 'app') IS NULL)
			THEN 0 /* Bad PRP logging in apps from Experience Services */
		WHEN ev.page_id IN (2487283) AND ev.web_server LIKE '%.ebay.com' AND lower(ev.url_query_string) LIKE '/ws/ebayisapi.dll?signinauthredirect=&guid=true%'
			THEN 0 /* Bad logging of Auth Redirects */
		WHEN ev.page_id IN (2380424) AND sojlib.soj_nvl(ev.soj, 'app') = '2571' AND ev.session_start_dt < '2019-04-25'
			THEN 0 /* Bad site ID tracking on Notification Hub page */
		WHEN ev.page_id IN (2050445) AND ev.web_server LIKE 'rover.ebay.%' AND lower(sojlib.soj_nvl(ev.soj, 'cguidsrc')) = 'new'
			THEN 0 /* Bad site ID tracking on Notification Hub page */
		WHEN ev.page_id NOT IN (2527563,2536688,2530661,3134835,4540640) AND lower(sojlib.soj_nvl(ev.soj, 'eactn')) = 'expm'
			THEN 0 /* EXPM events should be ignored, as they're module providers */
		WHEN ev.web_server LIKE '%.ebaystores.%'
			THEN 0 /* Applications are incorrectly using the ebaystores.tld domains, which have incorrect GUIDs */
		WHEN ev.page_id IN (2047675, 2349624) AND lower(ev.url_query_string) LIKE '%autorefresh%'
			THEN 0 /* VI auto-refresh is bleeding events for every server state refresh */
		WHEN ev.page_id IN (2323438) AND ev.session_start_dt BETWEEN '2018-02-15' AND '2099-12-31'
			THEN 0 /* C2C Vertical Selling Tiles always emitting a new GUID on every event */
		WHEN ev.page_id IN (2045573, 2053742) AND lower(ev.url_query_string) LIKE '/sch/ajax/predict%'
			THEN 0 /* Search AJAX Prediction */
		WHEN ev.web_server LIKE '%latest.%'
			THEN 0 /* Pre-production data */
		WHEN lower(ev.url_query_string) LIKE '%mpre%google.%asnc%'
			THEN 0 /* Google Parallel Tracking incorrect redirects */
		WHEN ev.page_id IN (2322147) AND lower(ev.url_query_string) LIKE '/findproduct/tracking%'
			THEN 0 /* fixing bad page ID for Catalog Search */
		WHEN lower(sojlib.soj_parse_clientinfo(ev.client_data, 'Agent')) LIKE 'swcd%'
			THEN 0 /* iOS Association file requests */
		WHEN sojlib.soj_nvl(ev.soj, 'rdthttps') IS NOT NULL AND sojlib.soj_nvl(ev.soj, 'sHit') IS NULL
			THEN 0 /* Search HTTPS redirect tracking */
		WHEN sojlib.soj_parse_clientinfo(ev.client_data, 'Agent') LIKE 'Dalvik/%Android%'
			THEN 0 /* Android VM calls - not real */
		WHEN ev.page_id = 2376473 AND sojlib.soj_nvl(ev.soj, 'app') in ('1462','2878') AND sojlib.soj_nvl(ev.soj, 'mav') IS NOT NULL AND sojlib.soj_replace_char(sojlib.soj_nvl(ev.soj, 'mav'), '01234567890.', '') = '' AND sojlib.soj_nvl(ev.soj, 'mav') LIKE ('5.%') AND CAST(sojlib.soj_list_get_val_by_idx(sojlib.soj_nvl(ev.soj, 'mav'), '\\.', 2) AS INTEGER) >= 36
			THEN 0 /* Eroneous CART landing page on iOS starting in 5.36 */
		WHEN ev.page_id = 1881 AND ev.seqnum = 1
			THEN 0 /* VI Revision Details GUID resets */
		WHEN ev.page_id IN (2058891,2057641) AND ev.site_id = 0 AND ev.web_server LIKE '%.stratus.%ebay.%'
			THEN 0 /* Bad watch/unwatch events */
		WHEN ev.page_id IN (2045573,2053742) AND (lower(sojlib.soj_nvl(ev.soj, 'sQr')) = 'update' OR lower(ev.url_query_string) = '/sch/update')
			THEN 0 /* Bad search dual-logging */
		WHEN ev.page_id IN (2380424) AND sojlib.soj_nvl(ev.soj, 'app') IN ('1462', '2878')
			THEN 0 /* Notification Hub iFrame on iOS added on Jul 20 2020*/
		WHEN ev.page_id IN (2065432) AND sojlib.soj_nvl(ev.soj, 'app') = '2571'
			THEN 0 /* Bell Notification iFrame on Android added on Jul 20 2020 */
		WHEN ev.page_id IN (2543464) AND LOWER(sojlib.soj_parse_clientinfo(ev.client_data, 'Agent')) LIKE ('%darwin%')
			THEN 0 /* Bad Agent Tracking on Error (SEO) */
		WHEN ev.page_id IN (3276719) AND LOWER(ev.url_query_string) LIKE '/sl/prelist/api/suggest%'
			THEN 0 /* Bad Prelist Suggest Tracking */
		WHEN sojlib.soj_nvl(ev.soj, 'efam') = 'LST' AND sojlib.soj_nvl(ev.soj, 'eactn') = 'SRCH'
			THEN 0 /* Bad Jetstream conversion double-logging */
		WHEN ev.page_id IN (3186120,3186125) AND lower(ev.url_query_string) LIKE '%ajax%'
			THEN 0 /* Overlogging on Report Downloads */
		WHEN ev.page_id IN (2380424) AND sojlib.soj_nvl(ev.soj, 'app') IN ('2571') AND ev.session_start_dt >= '2022-11-21' AND ev.site_id = 0
			THEN 0 /* Notification Hub iFrame on Android added on Nov 29 2022 */
		WHEN ev.page_id IN (3289402) AND sojlib.soj_nvl(ev.soj, 'poll') = 'true'
			THEN 0 /* M2M event inflation due to polling */
		WHEN ev.page_id IN (4375194,2481888) AND lower(ev.url_query_string) LIKE '/?_trkparms=%'
			THEN 0 /* Bad HP events */
		WHEN ev.page_id IN (-999) AND sojlib.soj_nvl(ev.soj, 'app') IN ('2571', '2878', '1462')
			THEN 0 /* List of page IDs that are valid on web, but not on native */
		WHEN ev.page_id IN (4451299) AND (sojlib.soj_nvl(ev.soj, 'app') IS NULL OR sojlib.soj_nvl(ev.soj, 'app') NOT IN ('2571', '2878', '1462'))
			THEN 0 /* List of page IDs that are valid on native, but not on web */
		ELSE 1
		END /* Exclude invalid pages */
	AS partial_valid_page,
	CASE
		WHEN (ev.url_query_string1 LIKE '/roverimp%' OR ev.url_query_string1 LIKE '%SojPageView%')
			THEN 1
		ELSE 0
	END AS cs_tracking, /* Client-side tracking */
	ev.u AS user_id,
	ev.bu AS best_guess_user_id,
	CAST(
		CASE
			WHEN CHAR_LENGTH(ev.n) = 32
				THEN ev.n
			ELSE NULL
		END AS STRING) AS cguid,
	ev.siid AS siid,
	CASE
		WHEN ev.page_id IN (1521826) AND ev.pgt IN ('future', 'like')
			THEN 'VI'
		WHEN ev.page_id IN (2066804) AND ev.url_query_string1 LIKE ANY ('/itm/like%', '/itm/future%')
			THEN 'VI'
		WHEN ev.page_id IN (1521826,2066804)
			THEN 'GR'
		WHEN ev.page_id IN (2385738)
			THEN 'VI'
		ELSE NULL
	END AS im_pgt,
	CASE
		WHEN ev.page_id IN (2588,3030,3907,4939,5108,5197,2050601) AND ev.url_query_string1 = '/?_js=OFF'
			THEN 1
		ELSE 0
	END AS non_js_hp,
	CAST(
		CASE
			WHEN ev.page_id IN (3818,3675,4546,4095,1187,3884,2640,3696,3993,3994,1822,4447,2608,2835,1787,2043032)
				THEN NULL
			WHEN ev.page_id = 3686 AND ev.url_query_string1 LIKE '%Portlet%'
				THEN NULL
			WHEN ev.remote_ip IS NOT NULL AND ev.remote_ip NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
				THEN ev.remote_ip
			WHEN ev.fwdfor1 IS NOT NULL AND ev.fwdfor1 NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
				THEN ev.fwdfor1
			WHEN ev.fwdfor2 IS NOT NULL AND ev.fwdfor2 NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
				THEN ev.fwdfor2
			WHEN ev.fwdfor3 IS NOT NULL AND ev.fwdfor3 NOT LIKE ALL ('10.%', '172.16.%', '192.168.%', '127.0.0.1','%,10.%', '%,172.16.%', '%,192.168.%', '%,127.0.0.1%')
				THEN ev.fwdfor3 ELSE NULL
		END AS STRING) AS external_ip,
	CAST(
		CASE
			WHEN ev.remote_ip IS NOT NULL
				THEN ev.remote_ip
			WHEN ev.fwdfor1 IS NOT NULL
				THEN ev.fwdfor1
			ELSE NULL
		END AS STRING) AS internal_ip,
	ev.user_name,
	CASE
		WHEN ev.referrer1 LIKE 'http://cs.ebay.%' OR ev.referrer1 LIKE 'https://cs.ebay.%'
			THEN 1
		ELSE 0
	END AS custserv_ref,
	CASE
		WHEN ev.url_query_string1 LIKE '%trackingNativeAppGuid=%'
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://xxx.ebay.com' || ev.url_query_string1), 'trackingNativeAppGuid')
		WHEN ev.referrer1 LIKE '%trackingNativeAppGuid=%'
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(ev.referrer1), 'trackingNativeAppGuid')
		WHEN ev.url_query_string1 LIKE ANY ('%&UUID=%', '%?UUID=%') AND CHAR_LENGTH(sojlib.soj_nvl(sojlib.soj_get_url_params('http://xxx.ebay.com' || ev.url_query_string1), 'UUID')) = 32
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://xxx.ebay.com' || ev.url_query_string1), 'UUID')
		WHEN ev.referrer1 LIKE ANY ('%&UUID=%', '%?UUID=%')
			THEN sojlib.soj_nvl(sojlib.soj_get_url_params(ev.referrer1), 'UUID')
		ELSE NULL
	END AS override_guid,
	CASE
		WHEN ev.referrer1 IS NULL
			THEN 0
		ELSE 1
	END AS has_ref,
	CAST(
		CASE
			WHEN ev.page_id IN (3818,2835,4409,4546)
				THEN NULL
			WHEN ev.agent_string LIKE ANY ('eBayNioHttpClient%', 'Apache-HttpClient/%', 'Shockwave Flash')
				THEN NULL
			WHEN sojlib.is_validipv4(ev.agent_string) = 1
				THEN NULL
			ELSE ev.agent_string
		END AS STRING) AS agent_string,
	CAST(
		CASE
			WHEN sojlib.is_decimal(ev.nid, 18, 0) = 1
				THEN ev.nid
			WHEN sojlib.is_decimal(sojlib.soj_list_get_val_by_idx(ev.nid, ',', 1), 18, 0) = 1
				THEN ev.nid
			WHEN sojlib.is_decimal(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(ev.nid, '%'), ',', 1), 18, 0) = 1
				THEN sojlib.soj_url_decode_escapes(ev.nid, '%')
			ELSE NULL
		END AS STRING) AS notification_id,
	CAST(
		CASE
			WHEN ev.page_id = 4486685 AND sojlib.soj_nvl(ev.soj,'blog_referrer') IS NOT NULL
				THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(ev.soj,'blog_referrer'), '%')
			WHEN ev.web_server LIKE ANY ('%rover.ebay.%', 'sofe.ebay.%') AND ev.url_query_string1 LIKE ANY ('%SojPageView%', '%roverimp%') AND ev.rurl LIKE ANY ('http://%', 'https://%')
				THEN ev.rurl
			WHEN ev.web_server LIKE ANY ('%rover.ebay.%', 'sofe.ebay.%') AND ev.url_query_string1 LIKE ANY ('%SojPageView%', '%roverimp%') AND sojlib.soj_url_decode_escapes(ev.rurl, '%') LIKE ANY ('http://%', 'https://%')
				THEN sojlib.soj_url_decode_escapes(ev.rurl, '%')
			WHEN ev.web_server LIKE ANY ('%rover.ebay.%', 'sofe.ebay.%') AND ev.url_query_string1 LIKE ANY ('%SojPageView%', '%roverimp%') AND sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(ev.rurl, '%'), '%') LIKE ANY ('http://%', 'https://%')
				THEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(ev.rurl, '%'), '%')
			WHEN ev.web_server LIKE ANY ('%rover.ebay.%', 'sofe.ebay.%') AND ev.url_query_string1 LIKE ANY ('%SojPageView%', '%roverimp%')
				THEN ev.rurl
			WHEN ev.referrer1 = 'null'
				THEN NULL
			WHEN ev.referrer1 LIKE ANY ('https://www.ebay.%/sw.js')
				THEN NULL
			WHEN ev.referrer1 LIKE ANY ('http://%', 'https://%')
				THEN ev.referrer1
			WHEN sojlib.soj_url_decode_escapes(ev.referrer1, '%') LIKE ANY ('http://%', 'https://%')
				THEN sojlib.soj_url_decode_escapes(ev.referrer1, '%')
			ELSE ev.referrer1
		END AS STRING) AS referrer,
	CAST(
		CASE
			WHEN ev.url_query_string1 LIKE '/roverimp%'
				THEN ev.referrer1
			WHEN ev.web_server LIKE 'pulsar.ebay.%' AND sojlib.soj_list_get_val_by_idx(ev.url_query_string1, '/', 3) = 'mpe'
				THEN ev.referrer1
			WHEN ev.page_id = 2380231 AND ev.referrer1 LIKE '%cdn.ampproject.org%'
				THEN ev.referrer1
			WHEN ev.page_id = 2208336 AND ev.referrer1 LIKE '%cdn.ampproject.org%' AND ev.session_start_dt BETWEEN '2016-09-16' AND '2016-10-20'
				THEN ev.referrer1
			ELSE 'https://' || COALESCE(ev.web_server, 'xxx.ebay.com') || ev.url_query_string1
		END AS STRING) AS page_url,
	ev.sid AS sid,
	ev.mppid,
	ev.mnt,
	ev.ort,
	CASE
		WHEN sojlib.is_decimal(sojlib.soj_nvl(ev.soj,'itm'),18,0) = 1
			THEN CAST(sojlib.soj_nvl(ev.soj,'itm') AS DECIMAL(18,0))
		WHEN sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string) LIKE '%/i/%' AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/'),18,0) = 1
			THEN CAST(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/') AS DECIMAL(18,0))
		WHEN sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string) LIKE '%/itm/%' AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/'),18,0) = 1
			THEN CAST(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/') AS DECIMAL(18,0))
		WHEN sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string) LIKE '%/p/%' AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params('http://x.ebay.com' || ev.url_query_string), 'itm'),18,0) = 1
			THEN CAST(sojlib.soj_nvl(sojlib.soj_get_url_params('http://x.ebay.com' || ev.url_query_string), 'itm') AS DECIMAL(18,0))
		ELSE NULL
	END AS item_id,
	CASE
		WHEN sojlib.is_decimal(sojlib.soj_nvl(ev.soj,'epid'),18,0) = 1
			THEN CAST(sojlib.soj_nvl(ev.soj,'epid') AS DECIMAL(18,0))
		WHEN sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string) LIKE '%/p/%' AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/'),18,0) = 1
			THEN CAST(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/') AS DECIMAL(18,0))
		ELSE NULL
	END AS epid,
	CAST(
		CASE
			WHEN ev.sQr IS NOT NULL
				THEN ev.sQr
			WHEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://rover.ebay.com' || ev.url_query_string), 'nkw') IS NOT NULL
				THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://rover.ebay.com' || ev.url_query_string), 'nkw')
			ELSE NULL
		END AS STRING) sqr2,
	CAST(
		CASE
			WHEN sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string) LIKE '/b/%' AND sojlib.is_decimal(replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/'),'bn_',''),18,0) = 1
				THEN replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path('http://x.ebay.com' || ev.url_query_string), '/'),'bn_','')
			ELSE NULL
		END AS STRING) bnid2
	FROM (
		SELECT
			CAST(
				CASE
					WHEN sojlib.is_integer(sojlib.soj_nvl(sojlib.soj_get_url_params('http://www.ebay.com' || ev2.url_query_string1), 'trackingNativeSrcAppId')) = 1
						THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://www.ebay.com' || lower(ev2.url_query_string1)), 'trackingNativeSrcAppId')
					WHEN sojlib.is_integer(sojlib.soj_nvl(sojlib.soj_get_url_params(ev2.referrer1), 'trackingNativeSrcAppId')) = 1
						THEN sojlib.soj_nvl(sojlib.soj_get_url_params(ev2.referrer1), 'trackingNativeSrcAppId')
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%;iPad%'
						THEN 2878
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%'
						THEN 1462
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'ebayUserAgent/eBayAndroid;%'
						THEN 2571
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'eBayAndroid/%'
						THEN 2571
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'eBayiPhone/%'
						THEN 1462
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'eBayiPad/%'
						THEN 2878
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'ebayUserAgent/ebayMotorsIOS%' T
						HEN 35023
					WHEN sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE 'ebayUserAgent/ebayMotorsAndroid%'
						THEN 35024
					WHEN sojlib.is_integer(sojlib.soj_nvl(ev2.soj, 'srcAppId')) = 1 AND sojlib.soj_nvl(ev2.soj, 'srcAppId') IN ('1462', '2571', '2878','35023','35024')
						THEN sojlib.soj_nvl(ev2.soj, 'srcAppId')
					WHEN sojlib.is_integer(sojlib.soj_nvl(ev2.soj, 'srcApp')) = 1 AND sojlib.soj_nvl(ev2.soj, 'srcApp') IN ('1462', '2571', '2878','35023','35024')
						THEN sojlib.soj_nvl(ev2.soj, 'srcApp')
					WHEN sojlib.is_integer(sojlib.soj_nvl(sojlib.soj_get_url_params('http://www.ebay.com' || ev2.url_query_string1), 'srcAppId')) = 1
						THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://www.ebay.com' || ev2.url_query_string1), 'srcAppId')
					WHEN ev2.url_query_string1 LIKE '%srcAppId%eBayInc80-8977-4f05-a933-3daa1311213%'
						THEN 1462
					WHEN ev2.url_query_string1 LIKE '%srcAppId%eBayInc64-7662-48ae-8d31-2b168593ee5%'
						THEN 2878
					WHEN ev2.url_query_string1 LIKE '%srcAppId%eBayInc52-907e-4b8a-ba0c-707469bb4d5%'
						THEN 2571
					WHEN ev2.url_query_string1 LIKE '%srcappid%eBayInc80-8977-4f05-a933-3daa1311213%'
						THEN 1462
					WHEN ev2.url_query_string1 LIKE '%srcappid%eBayInc64-7662-48ae-8d31-2b168593ee5%'
						THEN 2878
					WHEN ev2.url_query_string1 LIKE '%srcappid%eBayInc52-907e-4b8a-ba0c-707469bb4d5%'
						THEN 2571
					WHEN sojlib.is_integer(sojlib.soj_nvl(sojlib.soj_get_url_params(ev2.referrer1), 'srcAppId')) = 1
						THEN sojlib.soj_nvl(sojlib.soj_get_url_params(ev2.referrer1), 'srcAppId')
					WHEN sojlib.soj_nvl(ev2.soj, 'app') LIKE '%eBayInc80-8977-4f05-a933-3daa1311213%'
						THEN 1462
					WHEN sojlib.soj_nvl(ev2.soj, 'app') LIKE '%eBayInc64-7662-48ae-8d31-2b168593ee5%'
						THEN 2878
					WHEN sojlib.soj_nvl(ev2.soj, 'app') LIKE '%eBayInc52-907e-4b8a-ba0c-707469bb4d5%'
						THEN 2571
					WHEN sojlib.is_integer(sojlib.soj_nvl(ev2.soj, 'app')) = 1 AND sojlib.soj_nvl(ev2.soj, 'app') IN ('1462', '2571', '2878','35023','35024')
						THEN sojlib.soj_nvl(ev2.soj, 'app')
					WHEN ev2.url_query_string1 LIKE ANY ('%&UUID=%', '%?UUID=%') AND sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE '%iPad%'
						THEN 2878
					WHEN ev2.url_query_string1 LIKE ANY ('%&UUID=%', '%?UUID=%') AND sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE '%iPhone%'
						THEN 1462
					WHEN ev2.url_query_string1 LIKE ANY ('%&UUID=%', '%?UUID=%') AND sojlib.soj_parse_clientinfo(ev2.client_data, 'Agent') LIKE '%Android%'
						THEN 2571
					ELSE NULL
				END AS INTEGER) AS app_id,
				ev2.*
			FROM (
                    SELECT
                        ev3.guid,
                        ev3.session_skey,
                        ev3.event_timestamp,
                        ev3.seqnum,
                        CASE
                            WHEN ev3.web_server = 'www.ebay.coau' AND ev3.site_id = 0
                                THEN 15
                            WHEN ev3.web_server = 'in.ebay.com'
                                THEN 203
                            ELSE ev3.site_id
                        END AS site_id,
                        ev3.page_id,
                        ev3.page_name,
                        ev3.session_start_dt,
                        ev3.web_server,
                        CAST(sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(ev3.soj, 'sQr'), '%'), '%'), '%'), '+', ' ')) AS STRING) AS sQr,
                        CASE
                            WHEN sojlib.soj_nvl(ev3.soj, 'SigninRedirect') = 'V4'
                                THEN 1
                            ELSE ev3.rdt
                        END AS rdt1,
                        ev3.rdt,
                        CASE
                            WHEN sojlib.soj_nvl(ev3.soj, 'n2referrer') IS NOT NULL
                                THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(ev3.soj, 'n2referrer'), '%')
                            WHEN ev3.referrer = 'null'
                                THEN NULL
                            WHEN ev3.referrer LIKE ANY ('https://www.ebay.%/sw.js')
                                THEN NULL
                            ELSE ev3.referrer
                        END AS referrer1,
                        ev3.url_query_string,
                        CASE
                            WHEN ev3.url_query_string IS NULL
                                THEN '/'
                            WHEN ev3.url_query_string LIKE '/%'
                                THEN ev3.url_query_string
                            WHEN ev3.url_query_string LIKE '%/%'
                                THEN '/' || ev3.url_query_string
                            ELSE '/?' || COALESCE(ev3.url_query_string, '')
                        END AS url_query_string1,
                        CAST(
                            CASE
                                WHEN sojlib.is_integer(sojlib.soj_nvl(ev3.soj, 'pn')) = 1
                                    THEN sojlib.soj_nvl(ev3.soj, 'pn')
                                ELSE NULL
                            END AS INTEGER) AS pn,
                        CAST(sojlib.soj_nvl(ev3.soj, 'cflgs') AS STRING) AS cflgs,
                        CAST(sojlib.soj_nvl(ev3.soj, 'an') AS STRING) AS an,
                        CAST(sojlib.soj_nvl(ev3.soj, 'av') AS STRING) AS av,
                        CAST(sojlib.soj_nvl(ev3.soj, 'in') AS STRING) AS soj_in,
                        CAST(sojlib.soj_nvl(ev3.soj, 'mr') AS STRING) AS mr,
                        CAST(sojlib.soj_nvl(ev3.soj, 'state') AS STRING) AS soj_state,
                        CAST(sojlib.soj_nvl(ev3.soj, 'page') AS STRING) AS soj_page,
                        soj,
                        client_data,
                        CASE
                            WHEN sojlib.is_decimal(sojlib.soj_nvl(ev3.soj, 'u'), 18, 0) = 1
                                THEN CAST(sojlib.soj_nvl(ev3.soj, 'u') AS DECIMAL(18,0))
                            WHEN ev3.page_id IN (2508507,2368479,2239237,2255925,2056812,2368482,2500857,2523519,2546490,2523513) AND sojlib.is_decimal(sojlib.soj_nvl(ev3.soj, 'buyer_id'), 12, 0) = 1
                                THEN CAST(sojlib.soj_nvl(ev3.soj, 'buyer_id') AS DECIMAL(18,0))
                            ELSE NULL
                        END AS u,
                        CASE
                            WHEN sojlib.is_decimal(sojlib.soj_nvl(ev3.soj, 'bu'), 18, 0) = 1
                                THEN CAST(sojlib.soj_nvl(ev3.soj, 'bu') AS DECIMAL(18,0))
                            ELSE NULL
                        END AS bu,
                        TRIM(CAST(COALESCE(sojlib.soj_nvl(ev3.soj, 'user_name'), sojlib.soj_nvl(ev3.soj, 'userid'), sojlib.soj_nvl(sojlib.soj_get_url_params('http://xxx.ebay.com' || ev3.url_query_string), 'un'), sojlib.soj_nvl(sojlib.soj_get_url_params('http://xxx.ebay.com' || ev3.url_query_string), 'userName')) AS STRING)) AS user_name,
                        CAST(sojlib.soj_nvl(ev3.soj, 'n') AS STRING) AS n,
                        CAST(sojlib.soj_nvl(ev3.soj, 'siid') AS STRING) AS siid,
                        CAST(sojlib.soj_nvl(ev3.soj, 'pgt') AS STRING) AS pgt,
                        CAST(sojlib.soj_nvl(ev3.soj, 'nid') AS STRING) AS nid,
                        CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(ev3.soj, 'rurl'), '%') AS STRING) AS rurl,
                        CAST(sojlib.soj_nvl(ev3.soj, 'sid') AS STRING) AS sid,
                        CAST(sojlib.soj_nvl(ev3.soj, 'mppid') AS STRING) AS mppid,
                        CAST(sojlib.soj_nvl(ev3.soj, 'mnt') AS STRING) AS mnt,
                        CAST(sojlib.soj_nvl(ev3.soj, 'ort') AS STRING) AS ort,
                        CAST(sojlib.soj_parse_clientinfo(ev3.client_data, 'RemoteIP') AS STRING) AS remote_ip,
                        CAST(sojlib.soj_parse_clientinfo(ev3.client_data, 'Agent') AS STRING) AS agent_string,
                        CAST(sojlib.soj_parse_clientinfo(ev3.client_data, 'ForwardedFor') AS STRING) AS forwarded_for,
                        CAST(sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(ev3.client_data, 'ForwardedFor'), ',', 1) AS STRING) AS fwdfor1,
                        CAST(sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(ev3.client_data, 'ForwardedFor'), ',', 2) AS STRING) AS fwdfor2,
                        CAST(sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(ev3.client_data, 'ForwardedFor'), ',', 3) AS STRING) AS fwdfor3,
                        CAST(sojlib.soj_nvl(ev3.soj, 'pfn') AS STRING) AS pfn
                    FROM
                        ubi_v.ubi_event ev3
                        INNER JOIN p_soj_cl_t.outlier_sessions s
                        ON (ev3.guid = s.guid AND ev3.session_skey = s.session_skey AND ev3.session_start_dt = s.session_start_dt)
                    WHERE
                        s.soj_data_dt = '2024-06-01'
                        AND ev3.guid NOT IN ('','0ad8d2f617d6dbe1d9f7f40001991cf4','10fb48ba18b0a49ec7c5287affb4dee0','1a8926d6183f8531624000d00188ca5e','25c9c9ef184a8c6d771c29e001911a5b','4211452618f0ab39171af8a9ffff4a0e','5874094b18f0a4f3e21fb3c5fffe7536','59e7e6d318a0acf08fe5b5a7fffe76d4','5da98dc518d6ea662981b1e001332cef','7271a35a18a469da32e1bf10012c15fb','83e349531841cd18add0c5c0019b0bc8','86772aae18e77719fda15bd001f9cdbf','92714b8d18e0aa3bbc2243a8fffe4efe','9836df7318e930c36d8aa21001d81925','aef03db918f0a8d9e61995f7fff94c83','bf6b45d618f5856f4967403001d21c1d','c8330eb318f0aaf55c6c9543fff5fd5f')
                        /* Remove skewed GUIDs from the query to cut down on long runtimes */
                        AND s.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
                        AND ev3.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
                        AND ev3.page_id NOT IN (2608,2627,2774,2835,3818,3831,3872,3914,3962,4078,4158,4531,4546,4803,4988,5660,2046301,2046301,2047686,2050460,2053444,2053904,2054464,2054487,2054513,2054529,2054713,2062225,2062300,2067603,2154155,2299321,2304152,2317508,2356359,2364840,2371265,2379655,2380519,2380677,2381387,2388028,2401347,2402999,2480390,2491469,2492140,2046301,2051248,2051249,2054081,2054099,2054121,2055586,2061516,2065261,2065432,2208336,2234838,2239415,2266111,2309593,2356359,2365001,2367320,2367355,2368342,2370942,2381081,2388028,2402999,2480969,2490627,2492537,2493970,2494428,2507874,2510907,2512161,2516342,2542739,2543405,2545874,2546138,2547208,2550233,2552115,2552134,2555220,2563228,2564091,3120320,3132111,3144444,3161826,3238421,3251802,3458402,3544544,3645741,3682979,3703510,3735026,3735027,3741494,3755718,3792362,3830594,4279154,4384583,4395338,4436455,4439528,4439722,4445022,4447291,4450530,4472078,4479417,4498353,4512203,4525464)
                        /* Remove common iFrames... they're being thrown away from calculations anyways */
                        AND ev3.page_id NOT IN (2616,2627,2835,3084,3085,3936,3962,3985,3994,4531,2046301,2053444,2054060,2054081,2054099,2054487,2056122,2056451,2057680,2060921,2061026,2067603,2140389,2154434,2208336,2259407,2309593,2317508,2321885,2356359,2367355,2368342,2370942,2376867,2379655,2380677,2388028,2402999,2403006,2492140,2500024,2500304,2502515,2507874,2515526,2530290,2542782,2544028,2545063,2552134)
                        /* Remove pages that are blacklisted from ubi_event_noskew on Hopper */
                        AND ev3.site_id IS NOT NULL
                        AND sojlib.soj_replace_char(ev3.guid, '01234567890abcdefABCDEF', '') = ''
                        /* Remove bad GUIDs */
                ) ev2
		) ev ;

-- 2. Delete duplicate rows from temp_csess1a2_v9
DELETE t1a2
FROM
	p_soj_cl_t.temp_csess1a2_v9 t1a2,
	p_soj_cl_t.temp_csess1a_v9 t1a
WHERE
	t1a2.guid = t1a.guid
	and t1a2.session_skey = t1a.session_skey
	and t1a2.site_id = t1a.site_id
	and t1a2.session_start_dt = t1a.session_start_dt
	and t1a2.page_id = t1a.page_id
	and t1a2.event_timestamp = t1a.event_timestamp
	and t1a2.seqnum = t1a.seqnum;

-- 3. Insert into temp_csess1a2_v9
INSERT INTO p_soj_cl_t.temp_csess1a_v9 SELECT * FROM p_soj_cl_t.temp_csess1a2_v9;