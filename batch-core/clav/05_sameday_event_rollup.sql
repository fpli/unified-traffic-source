DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1da_v9;
CREATE TABLE p_soj_cl_t.temp_csess1da_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	CASE
		WHEN e.override_site_id IS NOT NULL
			THEN e.override_site_id
		ELSE e.site_id
	END AS site_id,
	CASE
		WHEN e.override_cobrand_id IN (2,3,4,5,9,10)
			THEN e.override_cobrand_id
		WHEN e.app_id IN (1232,1085,1396)
			THEN 11 /* Desktop apps */
		WHEN e.app_id IN (1462,2571,2878,2573,3882,10509,2410,2805,3833,1099,3653,1115,1160,1161,35023,35024)
			THEN 6 /* Mobile apps */
		WHEN e.page_fmly2_name = 'EbayExpress'
			THEN 2
		WHEN e.page_fmly2_name = 'Half'
			THEN 1
		WHEN e.pn = '505'
			THEN 2
		WHEN e.pn = '506'
			THEN 3
		WHEN e.pn = '502'
			THEN 1
		WHEN e.pn = '507'
			THEN 4
		ELSE 0
	END AS cobrand,
	e.session_start_dt AS session_start_dt,
	SUM(
		CASE
			WHEN e.partial_valid_page = 0
				THEN 0
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN 0
			WHEN e.iframe = 1
				THEN 0
			WHEN e.page_id IN (2720,1892,1893,4008,3288,2015,4699,4859,2557882,2691,2491192,2364992,3289,2550030,2056976,2486706)
				THEN 0 /* Bot Blocker pages */
			ELSE 1
		END) AS valid_page_count,
	SUM(
		CASE
			WHEN (e.rdt = 0 OR (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')) AND e.partial_valid_page = 1 AND (e.page_fmly4_name = 'GR' OR e.im_pgt = 'GR')
				THEN 1
			ELSE 0
		END) AS gr_cnt,
	SUM(
		CASE
			WHEN e.rdt = 0 AND e.partial_valid_page = 1 AND e.page_fmly4_name = 'GR-1'
				THEN 1
			ELSE 0
		END) AS gr_1_cnt,
	SUM(
		CASE
			WHEN e.rdt = 0 AND e.partial_valid_page = 1 AND (e.page_fmly4_name IN ('GR/VI','VI') OR e.im_pgt = 'VI')
				THEN 1
			ELSE 0
		END) AS vi_cnt,
	SUM(
		CASE
			WHEN e.rdt = 0 AND e.partial_valid_page = 1 AND e.page_fmly4_name = 'HOME'
				THEN 1
			ELSE 0
		END) AS homepage_cnt,
	SUM(
		CASE
			WHEN e.rdt = 0 AND e.partial_valid_page = 1 AND e.page_fmly4_name IN ('MYEBAY', 'SM', 'SMP')
				THEN 1
			ELSE 0
		END) AS myebay_cnt,
	SUM(
		CASE
			WHEN e.rdt = 0 AND e.partial_valid_page = 1 AND e.page_fmly4_name = 'SIGNIN'
				THEN 1
			ELSE 0
		END) AS signin_cnt,
	SUM(
		CASE
			WHEN e.partial_valid_page = 0
				THEN 0
			WHEN e.iframe = 1
				THEN 0
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN 0
			WHEN e.siid IS NOT NULL
				THEN 1
			ELSE 0
		END) AS siid_cnt,
	SUM(
		CASE
			WHEN e.partial_valid_page = 0
				THEN 0
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN 0
			else e.non_js_hp
		END) AS nonjs_hp_cnt,
	CASE
		WHEN
			SUM(
				CASE
					WHEN e.custserv_ref = 1
						THEN 1
					ELSE 0
				END) = 1
			THEN 1
		ELSE 0
	END + CASE
				WHEN
					SUM(
						CASE
							WHEN e.partial_valid_page = 0
								THEN 0
							WHEN e.iframe = 1
								THEN 0
							WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
								THEN 0
							WHEN e.has_ref = 1
								THEN 1
							ELSE 0
						END) = 0
					THEN 8
				ELSE 0
			END AS session_flags64,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0 and e.page_id not IN (2508507,2368482)
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			WHEN e.user_id <= 0 OR e.user_id IN (1,3564)
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&u=' || CAST(e.user_id AS STRING)
		END) AS first_uid,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			WHEN e.best_guess_user_id <= 0 OR e.best_guess_user_id IN (1,3564) OR CHAR_LENGTH(e.best_guess_user_id) > 10
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&bu=' || CAST(e.best_guess_user_id AS STRING)
		END) AS first_bguid,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&cguid=' || e.cguid
		END) AS first_cguid,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			WHEN e.app_id <= 0
				THEN NULL
			WHEN e.app_id IN (1622,3564,4290) OR e.app_id IS NULL
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&app=' || CAST(e.app_id AS STRING)
		END) AS first_appid,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&intip=' || e.internal_ip
		END) AS first_internal_ip,
	MIN(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&extip=' || e.external_ip
		END) AS first_external_ip,
	MIN(
		CASE
			WHEN e.iframe = 1
				THEN NULL
			ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&extip2=' || e.external_ip
		END) AS first_external_ip2, /* ext ip ON redirect and invalid pages*/
	MIN('ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&un=' || e.user_name) AS first_username,
	MIN(
		CASE
			WHEN CHAR_LENGTH(e.override_guid) = 32
				THEN e.override_guid
			ELSE NULL
		END) AS override_guid,
	MAX(
		CASE
			WHEN e.partial_valid_page = 0
				THEN NULL
			WHEN e.iframe = 1
				THEN NULL
			WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
				THEN NULL
			ELSE CAST(e.agent_string AS STRING)
		END) AS agent_string,
	MIN(
		CAST(
			CASE
				WHEN e.partial_valid_page = 0
					THEN NULL
				WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
					THEN NULL
				WHEN e.iframe = 1
					THEN NULL /* iFrames */
				WHEN e.page_id IN (2720,1892,1893,4008,3288,2015,4699,4859,2557882,2691,2491192,2364992,3289,2550030,2056976,2486706)
					THEN NULL /* Bot Blocker pages */
				ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&lp=' || CAST(e.page_id AS STRING) || '&sqn=' || CAST(e.seqnum AS STRING)
						   || CASE WHEN e.notification_id IS NULL THEN '' ELSE '&lnid=' || CAST(e.notification_id AS STRING) END
						   || CASE WHEN e.sid IS NULL THEN '' ELSE '&lsid=' || CAST(replace(replace(replace(e.sid, '%', '%25'), '&', '%26'), '=', '%3D') AS STRING) END
						   || CASE WHEN e.page_url IS NULL THEN '' ELSE '&url=' || CAST(replace(replace(replace(e.page_url, '%', '%25'), '&', '%26'), '=', '%3D') AS STRING) END
						   || CASE WHEN e.mppid IS NULL THEN '' ELSE '&mppid=' || CAST(e.mppid AS STRING) END
						   || CASE WHEN e.mnt IS NULL THEN '' ELSE '&mnt=' || CAST(e.mnt AS STRING) END
						   || CASE WHEN e.ort IS NULL THEN '' ELSE '&ort=' || CAST(e.ort AS STRING) END
						   || CASE WHEN e.agent_string IS NULL THEN '' ELSE '&agent=' || CAST(replace(replace(replace(e.agent_string, '%', '%25'), '&', '%26'), '=', '%3D') AS STRING) END
						   || CASE WHEN e.referrer IS NULL THEN '' ELSE '&ref=' || CAST(replace(replace(replace(e.referrer, '%', '%25'), '&', '%26'), '=', '%3D') AS STRING) END
			END AS STRING)) AS first_page,
	MAX(
		CAST(
			CASE
				WHEN e.partial_valid_page = 0
					THEN NULL
				WHEN e.rdt = 1 AND NOT (e.page_id IN (2045573,2046732,2045576,2189159) AND e.session_start_dt BETWEEN '2019-09-11' AND '2019-09-19')
					THEN NULL
				WHEN e.iframe = 1
					THEN NULL /* iFrames */
				WHEN e.page_id IN (2720,1892,1893,4008,3288,2015,4699,4859,2557882,2691,2491192,2364992,3289,2550030,2056976,2486706)
					THEN NULL /* Bot Blocker pages */
				ELSE 'ts=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS') || '&ep=' || CAST(e.page_id AS STRING) || '&sqn=' || CAST(e.seqnum AS STRING)
						   || CASE WHEN e.agent_string IS NULL THEN '' ELSE '&agent=' || CAST(replace(replace(replace(e.agent_string, '%', '%25'), '&', '%26'), '=', '%3D') AS STRING) END
		  	END AS STRING)) AS last_page
FROM
	p_soj_cl_t.temp_csess1c_v9 e
GROUP BY 1,2,3,4,5
HAVING valid_page_count > 0;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1db_v9;
CREATE TABLE p_soj_cl_t.temp_csess1db_v9 USING PARQUET AS
SELECT
	e.guid,
	e.session_skey,
	CASE
		WHEN e.override_site_id IS NOT NULL
			THEN e.override_site_id
		ELSE e.site_id
	END AS site_id,
	CASE
		WHEN e.override_cobrand_id IN (2,3,4,5,9,10)
			THEN e.override_cobrand_id
		WHEN e.app_id IN (1232,1085,1396)
			THEN 11 /* Desktop apps */
		WHEN e.app_id IN (1462,2571,2878,2573,3882,10509,2410,2805,3833,1099,3653,1115,1160,1161,35023,35024)
			THEN 6 /* Mobile apps */
		WHEN e.app_id IN (35023,35024)
			THEN 6 /* Motors apps */
		WHEN e.page_fmly2_name = 'EbayExpress'
			THEN 2
		WHEN e.page_fmly2_name = 'Half'
			THEN 1
		WHEN e.pn = '505'
			THEN 2
		WHEN e.pn = '506'
			THEN 3
		WHEN e.pn = '502'
			THEN 1
		WHEN e.pn = '507'
			THEN 4
		ELSE 0
	END AS cobrand,
	e.session_start_dt AS session_start_dt,
	array_sort(collect_list((e.event_timestamp as ts, e.seqnum as seq, e.page_id as pid, e.sid as sid, sojlib.soj_get_url_domain(e.referrer) as rd, CASE WHEN e.item_id is not null then 'itm' WHEN e.sqr2 is not null then 'sqr' WHEN e.bnid2 is not null then 'bnid' WHEN e.epid is not null then 'epid' else 'none' end as id_type, CASE WHEN e.item_id is not null then e.item_id WHEN e.sqr2 is not null then e.sqr2 WHEN e.bnid2 is not null then e.bnid2 WHEN e.epid is not null then e.epid else -999 end as id_val))) as page_list
FROM
	p_soj_cl_t.temp_csess1c_v9 e
WHERE
	partial_valid_page = 1
	AND (rdt = 0 OR (page_id IN (2045573,2046732,2045576,2189159) AND session_start_dt BETWEEN '2019-09-11' AND '2019-09-19'))
	AND iframe = 0
	AND e.page_id NOT IN (2720,1892,1893,4008,3288,2015,4699,4859,2557882,2691,2491192,2364992,3289,2550030,2056976,2486706) /* Bot Blocker pages */
group by 1,2,3,4,5;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1d_v9;
CREATE TABLE p_soj_cl_t.temp_csess1d_v9 USING PARQUET AS
select
	x.*,
	y.page_list,
	size(y.page_list) as page_list_cnt
FROM
	p_soj_cl_t.temp_csess1da_v9 x
	left outer join p_soj_cl_t.temp_csess1db_v9 y
	on x.guid = y.guid and x.session_skey = y.session_skey and x.session_start_dt = y.session_start_dt and x.site_id = y.site_id and x.cobrand = y.cobrand;