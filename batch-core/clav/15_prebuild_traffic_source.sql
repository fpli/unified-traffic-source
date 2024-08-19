UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a_v9 t2a,
	(
		SELECT
			t2a.guid,
			t2a.session_skey,
			t2a.site_id,
			t2a.cobrand,
			t2a.session_start_dt,
			CAST(MIN(
					CASE
						WHEN mc.page_id IN (2547208)
							THEN
								CASE
									WHEN mc.src_string IS NOT NULL
										THEN mc.src_string
									ELSE ''
								END
								|| CASE
										WHEN mc.mpx_chnl_id IS NOT NULL
											THEN '&mpc=' || CAST(mc.mpx_chnl_id AS STRING)
										ELSE ''
									END
								|| CASE WHEN mc.channel IS NOT NULL
											THEN '&rc=' || CAST(mc.channel AS STRING)
										ELSE ''
									END
								|| CAST(CASE WHEN mc.ref_domain IS NOT NULL THEN '&rd=' || mc.ref_domain ELSE '' END AS STRING)
								|| CAST(CASE WHEN mc.bought_keyword IS NOT NULL AND TRIM(mc.bought_keyword) <> '' THEN '&skw=' || TRIM(replace(replace(replace(mc.bought_keyword, '%', '%25'), '&', '%26'), '=', '%3D')) ELSE '' END AS STRING)
								|| CAST(CASE WHEN mc.campaign_id IS NOT NULL AND sojlib.is_decimal(mc.campaign_id, 18, 0) = 1 THEN '&camp=' || mc.campaign_id ELSE '' END AS STRING)
								|| CAST(CASE WHEN mc.crlp IS NOT NULL THEN '&crlp=' || mc.crlp ELSE '' END AS STRING)
								|| CAST(CASE WHEN mc.geo_id IS NOT NULL AND sojlib.is_integer(mc.geo_id) = 1 THEN '&geo=' || mc.geo_id ELSE '' END AS STRING)
								|| CAST(CASE WHEN mc.bought_keyword IS NOT NULL THEN '&bk=' || TRIM(replace(replace(replace(mc.bought_keyword, '%', '%25'), '&', '%26'), '=', '%3D')) ELSE '' END AS STRING)
						ELSE NULL
					END) AS STRING) AS mcs_entry_src_string
		FROM
			p_soj_cl_t.temp_csess2a_v9 t2a
			INNER JOIN p_soj_cl_t.temp_csess11a_v9 mc
			ON (t2a.guid = mc.guid AND t2a.session_skey = mc.session_skey AND t2a.session_start_dt = mc.session_start_dt AND mc.event_timestamp BETWEEN t2a.start_timestamp - INTERVAL '30' MINUTE AND t2a.start_timestamp + INTERVAL '60' SECOND)
		GROUP BY 1,2,3,4,5
	) mc1
SET
	t2a.mcs_entry_src_string = mc1.mcs_entry_src_string
WHERE
	t2a.guid = mc1.guid AND t2a.session_skey = mc1.session_skey AND t2a.site_id = mc1.site_id AND t2a.cobrand = mc1.cobrand AND t2a.session_start_dt = mc1.session_start_dt;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t2.guid,
			t2.session_skey,
			t2.site_id,
			t2.cobrand,
			t2.session_start_dt,
			CAST(MIN(
					CASE
						WHEN n.page_id IN (2054081,2046774) AND t2.primary_app_id = 2571
							THEN 'nvts=' || date_format(CAST(CAST(n.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
									     || CASE WHEN n.page_id IS NOT NULL THEN'&np=' || CAST(n.page_id AS STRING) ELSE '' END
									     || CAST(CASE WHEN n.user_id IS NOT NULL THEN '&nuid=' || CAST(n.user_id AS STRING)
												      WHEN n.user_name IS NOT NULL THEN '&nun=' || CAST(n.user_name AS STRING)
												      ELSE ''
											      END AS STRING)
									     || CAST(CASE WHEN n.event_type IS NOT NULL THEN '&net=' || CAST(n.event_type AS STRING)
										 			  WHEN n.event_type2 IS NOT NULL THEN '&net=' || CAST(n.event_type2 AS STRING)
													  ELSE ''
												  END AS STRING)
										 || CAST(CASE WHEN n.event_type IS NOT NULL THEN '&nsrc=soj'
										 			  WHEN n.event_type2 IS NOT NULL THEN '&nsrc=mts'
													  ELSE ''
												  END AS STRING)
										 || CASE WHEN n.app_name IS NOT NULL THEN '&napp=' || CAST(n.app_name AS STRING) ELSE '' END
										 || CASE WHEN n.notification_id IS NOT NULL THEN '&nid=' || CAST(n.notification_id AS STRING) ELSE '' END
										 || CASE WHEN LENGTH(n.utp_src_string) > 0 THEN '&' || n.utp_src_string ELSE '' END
										 || "¬_event_key=guid:"
										 || n.guid
										 || "|sskey:"
										 || CAST(n.session_skey AS STRING)
										 || "|seq:"
										 || CAST(n.seqnum AS STRING)
										 || "|ts:"
										 || date_format(CAST(CAST(n.event_timestamp AS TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
						ELSE NULL
					END) AS STRING) AS notif_viewed_src_string,
			CAST(MIN(
					CASE
						WHEN n.page_id = 2054060 AND n.pnact = 1
							THEN 'ncts=' || date_format(CAST(CAST(n.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
										 || CASE WHEN n.page_id IS NOT NULL THEN '&np=' || CAST(n.page_id AS STRING) ELSE '' END
										 || CASE WHEN n.pnact IS NOT NULL THEN '&pnact=' || CAST(n.pnact AS STRING) ELSE '' END
										 || CAST(CASE WHEN n.user_id IS NOT NULL
										                    THEN '&nuid=' || CAST(n.user_id AS STRING)
										              WHEN n.user_name IS NOT NULL
										                    THEN '&nun=' || CAST(n.user_name AS STRING)
										              ELSE ''
										         END AS STRING)
										 || CAST(CASE WHEN n.event_type IS NOT NULL
										                    THEN '&net=' || CAST(n.event_type AS STRING)
										              WHEN n.event_type2 IS NOT NULL
										                    THEN '&net=' || CAST(n.event_type2 AS STRING)
										              ELSE ''
										         END AS STRING)
										 || CAST(CASE WHEN n.event_type IS NOT NULL
										                    THEN '&nsrc=soj'
										              WHEN n.event_type2 IS NOT NULL
										                    THEN '&nsrc=mts'
										              ELSE ''
										         END AS STRING)
										 || CASE WHEN n.app_name IS NOT NULL THEN '&napp=' || CAST(n.app_name AS STRING) ELSE '' END
										 || CASE WHEN n.notification_id IS NOT NULL THEN '&nid=' || CAST(n.notification_id AS STRING) ELSE '' END
										 || CASE WHEN LENGTH(n.utp_src_string) > 0 THEN '&' || n.utp_src_string ELSE '' END
										 || "¬_event_key=guid:"
										 || n.guid
										 || "|sskey:"
										 || CAST(n.session_skey AS STRING)
										 || "|seq:"
										 || CAST(n.seqnum AS STRING)
										 || "|ts:"
										 || date_format(CAST(CAST(n.event_timestamp AS TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
						ELSE NULL END) AS STRING) AS notif_click_src_string,
			CAST(MIN(
					CASE WHEN n.page_id = 2054060 AND t2.primary_app_id = 2571 AND COALESCE(n.pnact,0) IN (0,2)
							THEN 'ndts=' || date_format(CAST(CAST(n.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
										 || CASE WHEN n.page_id IS NOT NULL THEN '&np=' || CAST(n.page_id AS STRING) ELSE '' END
										 || CASE WHEN n.pnact IS NOT NULL THEN '&pnact=' || CAST(n.pnact AS STRING) ELSE '' END
										 || CAST(CASE WHEN n.user_id IS NOT NULL
										                    THEN '&nuid=' || CAST(n.user_id AS STRING)
										              WHEN n.user_name IS NOT NULL
										                    THEN '&nun=' || CAST(n.user_name AS STRING)
										              ELSE ''
										         END AS STRING)
										 || CAST(CASE WHEN n.event_type IS NOT NULL
										                    THEN '&net=' || CAST(n.event_type AS STRING)
										              WHEN n.event_type2 IS NOT NULL
										                    THEN '&net=' || CAST(n.event_type2 AS STRING)
										              ELSE ''
										          END AS STRING)
										 || CAST(CASE WHEN n.event_type IS NOT NULL
										                    THEN '&nsrc=soj'
										              WHEN n.event_type2 IS NOT NULL
										                    THEN '&nsrc=mts'
										              ELSE ''
										         END AS STRING)
										 || CASE WHEN n.app_name IS NOT NULL THEN '&napp=' || CAST(n.app_name AS STRING) ELSE '' END
										 || CASE WHEN n.notification_id IS NOT NULL THEN '&nid=' || CAST(n.notification_id AS STRING) ELSE '' END
										 || CASE WHEN LENGTH(n.utp_src_string) > 0 THEN '&' || n.utp_src_string ELSE '' END
										 || "¬_event_key=guid:"
										 || n.guid
										 || "|sskey:"
										 || CAST(n.session_skey AS STRING)
										 || "|seq:"
										 || CAST(n.seqnum AS STRING)
										 || "|ts:"
										 || date_format(CAST(CAST(n.event_timestamp AS TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
						ELSE NULL END) AS STRING) AS notif_dismiss_src_string
		FROM
			p_soj_cl_t.temp_csess2a_v9 t2
			INNER JOIN p_soj_cl_t.temp_csess5a_v9 n
			ON (t2.guid = n.guid AND (n.event_timestamp BETWEEN t2.start_timestamp - INTERVAL '180' SECOND AND t2.start_timestamp + INTERVAL '3' SECOND))
		WHERE
			t2.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
			AND n.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
			AND COALESCE(n.event_type,n.event_type2,'999') <> 'INTERNAL_BADGE' /* Not Valid Event Types */
		GROUP BY 1,2,3,4,5
	) not1
SET
	notif_src_string = CASE WHEN not1.notif_click_src_string IS NOT NULL THEN not1.notif_click_src_string ELSE NULL END,
	nvts_src_string = not1.notif_viewed_src_string,
	ncts_src_string = not1.notif_click_src_string,
	ndts_src_string = not1.notif_dismiss_src_string
WHERE
	t2.guid = not1.guid AND t2.session_skey = not1.session_skey AND t2.site_id = not1.site_id AND t2.cobrand = not1.cobrand AND t2.session_start_dt = not1.session_start_dt;

UPDATE
p_soj_cl_t.temp_csess2a_v9
SET
	ref_domain = CAST(
		CASE
			WHEN referrer IS NULL AND ref_domain IS NOT NULL
				THEN ref_domain
			WHEN referrer LIKE '%pages.ebay.%/link/%'
				THEN NULL
			WHEN referrer like 'http://%'
				THEN sojlib.soj_str_between_endlist(referrer, 'http://', '\:/')
			WHEN referrer like 'https://%'
				THEN sojlib.soj_str_between_endlist(referrer, 'https://', '\:/')
			ELSE SUBSTR(sojlib.soj_get_url_domain(referrer), CHAR_LENGTH(sojlib.soj_get_url_domain(referrer))-99, 9999)
		END AS STRING),
	ref_keyword = CAST(
			TRIM(replace(sojlib.soj_url_decode_escapes(
														CASE
															WHEN referrer IS NULL
																THEN ''
															WHEN (sojlib.soj_get_url_domain(referrer) LIKE '%aolsearch%' OR sojlib.soj_get_url_domain(referrer) LIKE '%search.aol.%') AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'query') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'query')
															WHEN referrer LIKE '%google%/imgres?%prev=%'
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params('http://www.gooogle.com' || sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'prev'), '%')), 'q')
															WHEN referrer LIKE '%.yahoo.com/?p=us'
																THEN ''
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'q') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'q')
															WHEN referrer NOT LIKE '%.outbrain.com%' AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'p') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'p')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'query') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'query')
															WHEN referrer LIKE '%baidu%' AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'wd') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'wd')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'su') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'su')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'rdata') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'rdata')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'searchfor') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'searchfor')
															WHEN referrer LIKE '%www.ciao.%/sr/q-%'
																THEN SUBSTR(sojlib.soj_get_url_path(referrer), 7, 9999)
															WHEN referrer LIKE '%.shopping.com/istlo%' AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'mn') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'mn')
															WHEN referrer LIKE '%.shopping.com/xFS?%' AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'KW') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'KW')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'term') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'term')
															WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'text') IS NOT NULL
																THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'text')
															ELSE ''
														END,
													'%'),
						'+', ' ')
				) AS STRING),
	roverentry_ts = CASE WHEN sojlib.soj_nvl(roverentry_src_string, 't') IS NOT NULL AND sojlib.is_integer(sojlib.soj_nvl(roverentry_src_string, 't')) = 0 THEN CAST(sojlib.soj_nvl(roverentry_src_string, 't') AS TIMESTAMP) ELSE NULL END,
	roverns_ts = CASE WHEN sojlib.soj_nvl(roverns_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(roverns_src_string, 't') AS TIMESTAMP) ELSE NULL END,
	roveropen_ts = CASE WHEN sojlib.soj_nvl(roveropen_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(roveropen_src_string, 't') AS TIMESTAMP) ELSE NULL END,
	rtm_ts = CASE WHEN sojlib.soj_nvl(rtm_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(rtm_src_string, 't') AS TIMESTAMP) ELSE NULL END,
	notification_ts = CASE WHEN sojlib.soj_nvl(notif_src_string, 'ncts') IS NOT NULL THEN CAST(sojlib.soj_nvl(notif_src_string, 'ncts') AS TIMESTAMP) ELSE NULL END,
	mcs_entry_ts = CASE WHEN sojlib.soj_nvl(mcs_entry_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(mcs_entry_src_string, 't') AS TIMESTAMP) ELSE NULL END;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			t21.guid,
			t21.session_skey,
			t21.site_id,
			t21.cobrand,
			t21.min_sc_seqnum,
			MIN(t22.min_sc_seqnum) AS first_seqnum
		FROM
			p_soj_cl_t.temp_csess2a_v9 t21
			INNER JOIN p_soj_cl_t.temp_csess2a_v9 t22
			ON (t21.guid = t22.guid AND t21.session_skey = t22.session_skey AND t21.session_start_dt = t22.session_start_dt AND t21.min_sc_seqnum > t22.min_sc_seqnum)
		GROUP BY 1,2,3,4,5
	) already_on
SET
	t2.on_ebay_sess = 1,
	t2.first_seqnum = already_on.first_seqnum
WHERE
	t2.guid = already_on.guid AND t2.session_skey = already_on.session_skey AND t2.site_id = already_on.site_id AND t2.cobrand = already_on.cobrand AND t2.min_sc_seqnum = already_on.min_sc_seqnum;


COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;