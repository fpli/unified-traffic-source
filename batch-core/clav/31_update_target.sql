CREATE TABLE p_soj_cl_t.temp_clav_session_bu20240602075528 USING PARQUET AS
SELECT
	s.*
FROM
	p_soj_cl_t.clav_session s
WHERE
	s.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01';

ALTER TABLE p_soj_cl_t.clav_session DROP IF EXISTS PARTITION (session_start_dt >= '2024-05-30',session_start_dt <= '2024-06-01');

INSERT INTO p_soj_cl_t.clav_session
(
  guid,
  session_skey,
  session_start_dt,
  site_id,
  cobrand,
  cguid,
  lndg_page_id,
  start_timestamp,
  end_timestamp,
  exit_page_id,
  valid_page_count,
  gr_cnt,
  gr_1_cnt,
  vi_cnt,
  homepage_cnt,
  myebay_cnt,
  signin_cnt,
  min_sc_seqnum,
  max_sc_seqnum,
  signedin_user_id,
  mapped_user_id,
  primary_app_id,
  agent_id,
  session_cntry_id,
  session_rev_rollup,
  ip,
  bot_session,
  bot_flags64,
  exclude,
  exclude_v1,
  session_traffic_source_id,
  session_traffic_source_dtl,
  session_details,
  device_type,
  updated_ts,
  parent_uid
)
SELECT
  guid,
  session_skey,
  session_start_dt,
  CAST(site_id AS SMALLINT) AS site_id,
  cobrand,
  cguid,
  lndg_page_id,
  start_timestamp,
  end_timestamp,
  exit_page_id,
  valid_page_count,
  gr_cnt,
  gr_1_cnt,
  vi_cnt,
  homepage_cnt,
  myebay_cnt,
  signin_cnt,
  min_sc_seqnum,
  max_sc_seqnum,
  signedin_user_id,
  mapped_user_id,
  primary_app_id,
  agent_id,
  session_cntry_id,
  session_rev_rollup,
  ip,
  bot_session,
  bot_flags64,
  CASE
  		WHEN session_start_dt >= '2019-01-01'
			then bot_session
		WHEN bot_session = 1
			THEN 1
		WHEN cobrand = 6 AND primary_app_id NOT IN (3564, 4290) AND valid_page_count = 1
			THEN 0
		WHEN (signedin_user_id IS NOT NULL OR mapped_user_id IS NOT NULL) AND valid_page_count > 0
			THEN 0
		WHEN valid_page_count <= 1
			THEN 1
		ELSE 0
	END AS exclude,
  CASE
  		WHEN bot_session = 1
			THEN 1
		WHEN cobrand = 6 AND primary_app_id NOT IN (3564, 4290) AND valid_page_count = 1
			THEN 0
		WHEN (signedin_user_id IS NOT NULL OR mapped_user_id IS NOT NULL) AND valid_page_count > 0
			THEN 0
		WHEN valid_page_count <= 1
			THEN 1
		ELSE 0
	END AS exclude_v1,
  session_traffic_source_id,
  CAST(COALESCE(session_traffic_source_details, '') || COALESCE(session_traffic_source_dtl2, '') AS STRING) AS session_traffic_source_dtl,
  session_details,
  device_type,
  CURRENT_TIMESTAMP,
  parent_uid
FROM
 p_soj_cl_t.temp_csess3a_v9 t3a;