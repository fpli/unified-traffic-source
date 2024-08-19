DROP TABLE IF EXISTS p_soj_cl_t.temp_csess14a_v9;
CREATE TABLE p_soj_cl_t.temp_csess14a_v9 USING parquet AS
SELECT
	a.*,
	CAST(
		CASE
			WHEN sojlib.is_decimal(sojlib.soj_replace_char(sojlib.soj_nvl(mkevt_params,'mkrid') , '-', ''), 18,0) = 1
				THEN sojlib.soj_nvl(mkevt_params,'mkrid')
			ELSE NULL
		END AS STRING) AS rotation_string
FROM (
		SELECT
			s.guid,
			s.session_skey,
			s.site_id,
			s.session_start_dt,
			s.cobrand,
			s.primary_app_id,
			s.cguid,
			s.start_timestamp,
			MAX(
				CASE
					WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(lndg_page_url),'ru'),'%'),'%') like '%mkevt=1%'
					        AND sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(lndg_page_url),'ru'),'%'),'%') like '%mkcid%'
						THEN sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(lndg_page_url),'ru'),'%'),'%'))
					WHEN s.lndg_page_url like '%mkevt%'
						THEN sojlib.soj_get_url_params(replace(s.lndg_page_url,'amp;',''))
					WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(lndg_page_url),'ru'),'%'),'%') like '%mkcid%'
						THEN sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(lndg_page_url),'ru'),'%'),'%'))
					WHEN s.lndg_page_url like '%mkcid%'
						THEN sojlib.soj_get_url_params(replace(s.lndg_page_url,'amp;',''))
					ELSE NULL
				END) as mkevt_params
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
		WHERE
		    s.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
		group by 1,2,3,4,5,6,7,8
		HAVING mkevt_params IS NOT NULL
	) a;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess14b_v9;
CREATE TABLE p_soj_cl_t.temp_csess14b_v9 USING parquet AS
SELECT
	a.*,
	r.mpx_chnl_id,
	r.rotation_id,
	CAST(
		CASE
			WHEN sojlib.is_integer(sojlib.soj_nvl(mkevt_params,'mkcid')) = 1
				THEN sojlib.soj_nvl(mkevt_params,'mkcid')
			ELSE NULL
		END AS INTEGER) AS channel,
	CAST(sojlib.soj_nvl(mkevt_params,'gclid') AS STRING) AS gclid,
	CAST(replace(CASE
	                WHEN mkevt_params LIKE '%keyword=%'
	                    THEN sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(mkevt_params, 'keyword'), '%'), '+', ' '))
	                WHEN mkevt_params LIKE '%rawquery=%'
	                    THEN sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(mkevt_params, 'rawquery'), '%'), '+', ' '))
	                ELSE NULL END, '&', '%26') AS STRING) AS bought_keyword,
	CAST(COALESCE(sojlib.soj_nvl(mkevt_params, 'campid'), sojlib.soj_nvl(mkevt_params, 'amp;campid')) AS STRING) AS campaign_id,
	CAST(sojlib.soj_nvl(mkevt_params, 'crlp') AS STRING) AS crlp,
	CAST(sojlib.soj_nvl(mkevt_params, 'geo_id') AS STRING) AS geo_id,
	CAST(sojlib.soj_nvl(mkevt_params, 'segname') AS STRING) AS segname,
	CAST(sojlib.soj_nvl(mkevt_params, 'sabg') AS STRING) AS sabg,
	CAST(sojlib.soj_nvl(mkevt_params, 'bannerrid') AS STRING) AS bannerrid,
	CAST(sojlib.soj_nvl(mkevt_params, 'bannercid') AS STRING) AS bannercid,
	CAST(sojlib.soj_nvl(mkevt_params, 'trkId') AS STRING) AS trkid,
	CAST(sojlib.soj_nvl(mkevt_params, 'mesgId') AS STRING) AS mesgid,
	CAST(sojlib.soj_nvl(mkevt_params, 'plmtId') AS STRING) AS plmtid,
	CAST(sojlib.soj_nvl(mkevt_params, 'recoId') AS STRING) AS recoid,
	CAST(sojlib.soj_nvl(mkevt_params, 'recoPos') AS STRING) AS recopos,
	CAST(sojlib.soj_nvl(mkevt_params, 'cnvId') AS STRING) AS cnvid,
	CAST(sojlib.soj_nvl(mkevt_params, 'crd') AS STRING) AS crd,
	CAST(sojlib.soj_nvl(mkevt_params, 'euid') AS STRING) AS euid,
	CAST(sojlib.soj_nvl(mkevt_params, 'emid') AS STRING) AS emid,
	CAST(sojlib.soj_nvl(mkevt_params, 'emsid') AS STRING) AS emsid,
	CAST(sojlib.soj_nvl(mkevt_params, 'email') AS STRING) AS email,
	CAST(sojlib.soj_nvl(mkevt_params, 'ext') AS STRING) AS ext,
	CAST(sojlib.soj_nvl(mkevt_params, 'co') AS STRING) AS co,
	CAST(sojlib.soj_nvl(mkevt_params, 'ymmmid') AS STRING) AS ymmmid,
	CAST(sojlib.soj_nvl(mkevt_params, 'ymsid') AS STRING) AS ymsid,
	CAST(sojlib.soj_nvl(mkevt_params, 'yminstc') AS STRING) AS yminstc,
	CAST(sojlib.soj_nvl(mkevt_params, 'mktype') AS STRING) AS mktype
FROM
	p_soj_cl_t.temp_csess14a_v9 a
	LEFT OUTER JOIN (
						SELECT
							rotation_string,
							MIN(rotation_id) AS rotation_id,
							MIN(campaign_id) AS campaign_id,
							MIN(mpx_chnl_id) AS mpx_chnl_id
						FROM
							choco_data_v.dw_mpx_rotations
						GROUP BY 1
					) r
	ON r.rotation_string = a.rotation_string AND a.rotation_string <> '0';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess14c_v9;
CREATE TABLE p_soj_cl_t.temp_csess14c_v9 USING DELTA AS
SELECT
	lp.*,
	CAST('t=' ||CAST(lp.start_timestamp AS STRING)
		|| CASE WHEN lp.rotation_id IS NULL THEN '' ELSE '&rot=' || sojlib.soj_replace_char(CAST(lp.rotation_id AS STRING),'\.','') END
		|| CASE WHEN lp.mpx_chnl_id IS NOT NULL THEN '&mpc=' || CAST(lp.mpx_chnl_id AS STRING) ELSE '' END
		|| CASE WHEN lp.channel IS NOT NULL THEN '&rc=' || CAST(lp.channel AS STRING) ELSE '' END
		|| CASE WHEN lp.mktype IS NOT NULL THEN '&mktype=' || CAST(lp.mktype AS STRING) ELSE '' END
		|| CAST(CASE WHEN lp.campaign_id IS NOT NULL AND sojlib.is_decimal(lp.campaign_id, 18, 0) = 1 THEN '&camp=' || lp.campaign_id ELSE '' END AS STRING)
		|| CAST(CASE WHEN lp.crlp IS NOT NULL THEN '&crlp=' || lp.crlp ELSE '' END AS STRING)
		|| CAST(CASE WHEN lp.geo_id IS NOT NULL AND sojlib.is_integer(lp.geo_id) = 1 THEN '&geo=' || lp.geo_id ELSE '' END AS STRING)
		|| CAST(CASE WHEN lp.bought_keyword IS NOT NULL THEN '&bk=' || TRIM(replace(replace(replace(lp.bought_keyword, '%', '%25'), '&', '%26'), '=', '%3D')) ELSE '' END AS STRING)
		|| CAST(CASE WHEN lp.bought_keyword IS NOT NULL THEN '&skw=' || TRIM(replace(replace(replace(lp.bought_keyword, '%', '%25'), '&', '%26'), '=', '%3D')) ELSE '' END AS STRING)
		|| CASE WHEN segname IS NOT NULL THEN '&segname=' || CAST(segname AS STRING) ELSE '' END
		|| CASE WHEN sabg IS NOT NULL THEN '&sabg=' || CAST(sabg AS STRING) ELSE '' END
		|| CASE WHEN bannercid IS NOT NULL THEN '&bannercid=' || CAST(bannercid AS STRING) ELSE '' END
		|| CASE WHEN bannerrid IS NOT NULL THEN '&bannerrid=' || CAST(bannerrid AS STRING) ELSE '' END
		|| CASE WHEN trkid IS NOT NULL THEN '&trkid=' || CAST(trkid AS STRING) ELSE '' END
		|| CASE WHEN mesgid IS NOT NULL THEN '&mesgid=' || CAST(mesgid AS STRING) ELSE '' END
		|| CASE WHEN plmtid IS NOT NULL THEN '&plmtid=' || CAST(plmtid AS STRING) ELSE '' END
		|| CASE WHEN recoid IS NOT NULL THEN '&recoid=' || CAST(recoid AS STRING) ELSE '' END
		|| CASE WHEN recopos IS NOT NULL THEN '&recopos=' || CAST(recopos AS STRING) ELSE '' END
		|| CASE WHEN cnvid IS NOT NULL THEN '&cnvid=' || CAST(cnvid AS STRING) ELSE '' END
		|| CASE WHEN channel IN (7,8) THEN CASE WHEN CHAR_LENGTH(euid) = 32 THEN '&euid=' || CAST(euid AS STRING) ELSE '' END
                                            || CASE WHEN sojlib.is_bigint(emid)= 1 THEN '&emid=' || CAST(emid AS STRING) ELSE '' END
                                            || CASE WHEN sojlib.is_bigint(email)= 1 THEN '&email=' || CAST(email AS STRING) ELSE '' END
                                            || CASE WHEN emsid LIKE 'e%.%' THEN '&e=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(emsid, '\\.', 1), 2, 9999) AS STRING)
                                                    WHEN emsid LIKE 'e%' THEN '&e=' || CAST(SUBSTR(emsid, 2, 9999) AS STRING)
                                                    ELSE '' END
                                            || CASE WHEN emsid LIKE '%.m%.%'
                                                        THEN CASE
                                                                WHEN sojlib.soj_list_get_val_by_idx(emsid, '\\.', 2) LIKE 'm%' THEN '&m=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(emsid, '\\.', 2), 2, 7) AS STRING)
                                                                WHEN sojlib.soj_list_get_val_by_idx(emsid, '\\.', 3) LIKE 'm%' THEN '&m=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(emsid, '\\.', 3), 2, 7) AS STRING)
                                                                ELSE ''
                                                             END
                                                    ELSE ''
                                                END
                                            || CASE WHEN emsid LIKE '%.l%'
                                                        THEN CASE
                                                                WHEN sojlib.soj_list_get_val_by_idx(emsid, '\\.', 3) LIKE 'l%' THEN '&l=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(emsid, '\\.', 3), 2, 7) AS STRING)
                                                                WHEN sojlib.soj_list_get_val_by_idx(emsid, '\\.', 4) LIKE 'l%' THEN '&l=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(emsid, '\\.', 4), 2, 7) AS STRING)
                                                                ELSE ''
                                                              END
                                                    ELSE ''
                                                END
                                            || CASE WHEN sojlib.is_bigint(ext)= 1 THEN '&ext=' || CAST(ext AS STRING) ELSE '' END
                                            || CASE WHEN CHAR_LENGTH(crd) = 14 THEN '&crd=' || CAST(crd AS STRING) ELSE '' END
                                            || CASE WHEN co IS NOT NULL THEN '&co=' || CAST(co AS STRING) ELSE '' END
                                            || CASE WHEN ymmmid IS NOT NULL THEN '&ymmmid=' || CAST(ymmmid AS STRING) ELSE '' END
                                            || CASE WHEN ymmmid IS NOT NULL THEN '&ymsid=' || CAST(ymsid AS STRING) ELSE '' END
                                            || CASE WHEN ymmmid IS NOT NULL THEN '&yminstc=' || CAST(yminstc AS STRING) ELSE '' END
				ELSE ''
			END AS STRING) as src_string
FROM
	p_soj_cl_t.temp_csess14b_v9 lp;

UPDATE rc
FROM
	p_soj_cl_t.temp_csess14c_v9 rc,
	(
		SELECT
			b.AMS_PBLSHR_CMPGN_ID as campaign_id,
			min(coalesce(ap.partner_group, 'unclassified')) as partner_group
		from
			PRS_AMS_V.AMS_PBLSHR_CMPGN b
			inner join PRS_AMS_V.AMS_PBLSHR ap
			on b.AMS_PBLSHR_ID = ap.AMS_PBLSHR_ID
		group by 1
	) r
SET
	rc.src_string = case when rc.src_string is null then '' else rc.src_string end || '&epn_campid=' || CAST(rc.campaign_id AS STRING) || '&epn_group=' || CAST(case when r.partner_group is not null then r.partner_group else 'unmapped' end AS STRING)
WHERE
	r.campaign_id = rc.campaign_id and rc.mpx_chnl_id = 6 and rc.campaign_id is not null;

UPDATE t2a
FROM
	p_soj_cl_t.temp_csess2a_v9 t2a,
	p_soj_cl_t.temp_csess14c_v9 t14c
SET
	t2a.lndg_page_src_string = t14c.src_string,
	t2a.lndg_page_ts = CASE WHEN sojlib.soj_nvl(t14c.src_string, 't') IS NOT NULL THEN CAST(sojlib.soj_nvl(t14c.src_string, 't') AS TIMESTAMP) ELSE NULL END
WHERE
	t2a.guid = t14c.guid AND t2a.session_skey = t14c.session_skey AND t2a.session_start_dt = t14c.session_start_dt AND t2a.site_id = t14c.site_id AND t2a.cobrand = t14c.cobrand AND t14c.src_string IS NOT NULL;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;