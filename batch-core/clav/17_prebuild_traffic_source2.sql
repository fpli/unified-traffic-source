UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a_v9 t2a,
	p_soj_cl_t.temp_csess12b_v9 dl
SET
	t2a.dlk_entry_src_string = dl.dlk_src_string,
	t2a.dlk_entry_ts = CASE WHEN sojlib.soj_nvl(dl.dlk_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(dl.dlk_src_string, 't') AS TIMESTAMP) ELSE NULL END,
	t2a.dlk_referrer = dl.dlk_referrer,
	t2a.dlk_deeplink = dl.dlk_deeplink,
	t2a.dlk_brguid = dl.dlk_mweb_guid,
	t2a.dlk_brsess = dl.dlk_mweb_sess,
	t2a.dlk_mweb_link_type = dl.dlk_mweb_link_type,
	t2a.dlk_details = CASE WHEN dl.dlk_type IS NOT NULL THEN '&dltype=' || CAST(dl.dlk_type AS STRING) ELSE '' END
						|| CASE WHEN dl.dlk_gclid IS NOT NULL THEN '&gclid=' || CAST(dl.dlk_gclid AS STRING) ELSE '' END
						|| CASE WHEN dl.dlk_item_id IS NOT NULL THEN '&itm=' || CAST(CAST(dl.dlk_item_id AS BIGINT) AS STRING) ELSE '' END
						|| CASE WHEN dl.dlk_epid IS NOT NULL THEN '&epid=' || CAST(dl.dlk_epid AS STRING) ELSE '' END
						|| CASE WHEN dl.dlk_bnid IS NOT NULL THEN '&bnid=' || CAST(dl.dlk_bnid AS STRING) ELSE '' END
						|| CASE WHEN dl.dlk_sqr IS NOT NULL THEN '&sqr=' || CAST(dl.dlk_sqr AS STRING) ELSE '' END,
	t2a.session_details = COALESCE(session_details, '') || '&dlk=1'
WHERE
    t2a.guid = dl.guid AND t2a.session_skey = dl.session_skey AND t2a.site_id = dl.site_id;

UPDATE
	p_soj_cl_t.temp_csess2a_v9
SET
	referrer = COALESCE(referrer, dlk_referrer),
	ref_domain = COALESCE(ref_domain, sojlib.soj_get_url_domain(dlk_referrer)),
	dlk_entry_src_string = dlk_entry_src_string || CAST(CASE WHEN sojlib.soj_nvl(dlk_entry_src_string,'rd') IS NULL THEN '&rd=' || sojlib.soj_get_url_domain(dlk_referrer) ELSE '' END AS STRING)
WHERE
	dlk_referrer IS NOT NULL AND sojlib.soj_get_url_domain(dlk_referrer) NOT LIKE ('%.ebay.%');

UPDATE
	p_soj_cl_t.temp_csess2a_v9
SET
	ns_event_src_string = CASE
	                            WHEN dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') = 'roverns' AND primary_app_id = 2571 AND dlk_deeplink like 'ebay://link/?nav=item.view%' and dlk_referrer is NULL
	                                THEN NULL /* handle use case for pages with hard coded nsevent */
	                            WHEN roverns_src_string IS NOT NULL
	                                THEN roverns_src_string
	                            WHEN dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') = 'roverns'
	                                THEN dlk_entry_src_string
	                            ELSE NULL
	                      END,
	ns_event_ts = CASE
	                    WHEN dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') = 'roverns' AND primary_app_id = 2571 AND dlk_deeplink like 'ebay://link/?nav=item.view%' and dlk_referrer is NULL
	                        THEN NULL /* handle use case for pages with hard coded nsevent */
	                    WHEN roverns_src_string IS NOT NULL
	                        THEN roverns_ts
	                    WHEN dlk_entry_src_string IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') = 'roverns'
	                        THEN dlk_entry_ts
	                    ELSE NULL
	              END
WHERE
	roverns_ts IS NOT NULL OR (dlk_entry_ts IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') = 'roverns');

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a_v9 t2a,
	(
		SELECT
			guid,
			session_skey,
			session_start_dt,
			site_id,
			start_timestamp,
			event_type,
			entry_event_src_string,
			entry_event_ts,
			SUM(1) OVER (PARTITION BY guid, session_skey, session_start_dt, site_id ORDER BY entry_event_ts ROWS UNBOUNDED PRECEDING) AS dedupe_rank
		FROM (
				SELECT
					guid,
					session_skey,
					session_start_dt,
					site_id,
					start_timestamp,
					'dlk' AS event_type,
					dlk_entry_src_string AS entry_event_src_string,
					dlk_entry_ts AS entry_event_ts
				FROM
					p_soj_cl_t.temp_csess2a_v9
				WHERE
					dlk_entry_ts IS NOT NULL AND sojlib.soj_nvl(dlk_details,'dltype') IN ('rover','email','mkevt','sab') /*roverns is handled separately*/
				UNION ALL
				SELECT
					guid,
					session_skey,
					session_start_dt,
					site_id,
					start_timestamp,
					'mcs' AS event_type,
					mcs_entry_src_string AS entry_event_src_string,
					mcs_entry_ts AS entry_event_ts
				FROM
					p_soj_cl_t.temp_csess2a_v9
				WHERE
					mcs_entry_ts IS NOT NULL
				UNION ALL
				SELECT
					guid,
					session_skey,
					session_start_dt,
					site_id,
					start_timestamp,
					'rvr' AS event_type,
					roverentry_src_string AS entry_event_src_string,
					roverentry_ts AS entry_event_ts
				FROM
					p_soj_cl_t.temp_csess2a_v9
				WHERE
					roverentry_ts IS NOT NULL
			) y
		QUALIFY dedupe_rank = 1
	) x
SET
	t2a.entry_event_src_string = x.entry_event_src_string || '&etype=' || x.event_type,
	t2a.entry_event_ts = x.entry_event_ts
WHERE
	t2a.guid = x.guid AND t2a.session_skey = x.session_skey AND t2a.session_start_dt = x.session_start_dt AND t2a.site_id = x.site_id;

UPDATE
	p_soj_cl_t.temp_csess2a_v9 t2a
SET
	entry_event_src_string = lndg_page_src_string || '&etype=lndg',
	entry_event_ts = CASE WHEN sojlib.soj_nvl(lndg_page_src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(lndg_page_src_string, 't') AS TIMESTAMP) ELSE NULL END
WHERE
	lndg_page_src_string IS NOT NULL AND entry_event_src_string is NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;
