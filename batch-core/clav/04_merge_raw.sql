DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1b1_v9;
CREATE TABLE p_soj_cl_t.temp_csess1b1_v9 USING PARQUET AS
SELECT
	*
from
	p_soj_cl_t.temp_csess1a_v9
UNION ALL
SELECT
	*
from
	p_soj_cl_t.temp_csess1a5_v9;


DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1b_v9;
CREATE TABLE p_soj_cl_t.temp_csess1b_v9 USING PARQUET AS
SELECT
	hp.*
FROM (
		SELECT
			e.guid,
			e.session_skey,
			e.session_start_dt,
			MIN(
				CASE
					WHEN e.page_id IN (2051248, 2050535)
						THEN e.seqnum
					ELSE NULL
				END) AS first_fg_launch_seqnum,
			MIN(
				CASE
					WHEN e.page_id IN (2051248, 2050535)
						THEN e.event_timestamp
					ELSE NULL
				END) AS first_fg_launch_ts,
			MIN(
				CASE
					WHEN e.page_id = 2481888
						THEN e.seqnum
					ELSE NULL
				END) AS first_hp_seqnum,
			MIN(
				CASE
					WHEN e.page_id = 2481888
						THEN e.event_timestamp
					ELSE NULL
				END) AS first_hp_ts,
			MIN(
				CASE
					WHEN e.page_id = 3562572
						THEN e.seqnum
					ELSE NULL
				END) AS first_coll_seqnum,
			MIN(
				CASE
					WHEN e.page_id = 3562572
						THEN e.event_timestamp
					ELSE NULL
				END) AS first_coll_ts
			FROM
				ubi_v.ubi_event e
			WHERE
				e.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
				AND e.page_id IN (2051248,2051249,2367320,2481888,3562572)
				AND sojlib.soj_nvl(e.soj, 'app') IN ('1462', '2878') /* iOS App */
			GROUP BY 1,2,3
	) hp
WHERE
	hp.first_hp_seqnum IS NOT NULL
	AND (hp.first_hp_seqnum < hp.first_fg_launch_seqnum OR hp.first_fg_launch_seqnum IS NULL) ;

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess1c_v9;
CREATE TABLE p_soj_cl_t.temp_csess1c_v9 USING PARQUET AS
SELECT
	t1a.guid,
	t1a.session_skey,
	t1a.site_id,
	t1a.session_start_dt,
	t1a.page_id,
	t1a.event_timestamp,
	t1a.seqnum,
	t1a.rdt,
	t1a.pn,
	t1a.app_id,
	CASE
		WHEN t1b.guid IS NOT NULL
			THEN 0
		ELSE t1a.partial_valid_page
	END AS partial_valid_page,
	t1a.cs_tracking,
	t1a.user_id,
	t1a.best_guess_user_id,
	t1a.cguid,
	t1a.siid,
	t1a.im_pgt,
	t1a.non_js_hp,
	t1a.external_ip,
	t1a.internal_ip,
	t1a.user_name,
	t1a.custserv_ref,
	t1a.override_guid,
	t1a.has_ref,
	t1a.agent_string,
	t1a.notification_id,
	t1a.referrer,
	t1a.page_url,
	t1a.sid,
	t1a.item_id,
	t1a.epid,
	t1a.sqr2,
	t1a.bnid2,
	t1a.mppid,
	t1a.mnt,
	t1a.ort,
	pg.override_site_id,
	pg.override_cobrand_id,
	CASE
		WHEN (t1a.session_start_dt BETWEEN '2016-09-16' AND '2016-10-20' AND t1a.page_id = 2208336 AND t1a.referrer LIKE '%cdn.ampproject.org%')
			THEN 0
		ELSE pg.iframe
	END AS iframe,
	pg.page_fmly2_name,
	pg.page_fmly4_name
FROM
	p_soj_cl_t.temp_csess1b1_v9 t1a
	INNER JOIN p_soj_cl_v.pages pg
	ON (t1a.page_id = pg.page_id)
	LEFT OUTER JOIN p_soj_cl_t.temp_csess1b_v9 t1b
	ON (
	    t1b.guid = t1a.guid
	    AND t1b.session_skey = t1a.session_skey
	    AND t1b.session_start_dt = t1a.session_start_dt
	    AND t1b.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	    AND t1a.session_start_dt BETWEEN '2024-05-31' AND '2024-06-01'
	    AND t1a.page_id = 2481888
	    AND t1a.app_id IN (1462, 2878)
	    AND (
	            t1a.seqnum < t1b.first_fg_launch_seqnum
	            OR
	            (t1b.first_coll_seqnum is NOT NULL and t1a.seqnum = t1b.first_hp_seqnum and t1b.first_fg_launch_seqnum IS NULL)
	         )
	    )
WHERE
	(t1a.page_id in (2508507, 2368482) OR pg.iframe IS NULL OR pg.iframe = 0);