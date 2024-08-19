DROP TABLE IF EXISTS p_soj_cl_t.temp_pre2_5a_v1;
CREATE TABLE p_soj_cl_t.temp_pre2_5a_v1 USING DELTA AS
SELECT
  *
FROM
  (
    SELECT
      e.guid,
      e.session_start_dt,
      e.session_skey,
      e.seqnum,
      e.event_timestamp,
      CAST(
        CASE
          WHEN e.session_start_dt BETWEEN '2018-05-23' AND '2018-05-24' AND CAST(sojlib.soj_nvl(e.client_data, 'TPool') AS STRING) = 'r1rover' AND sojlib.soj_nvl(e.soj, 'es') IS NOT NULL AND sojlib.is_integer(sojlib.soj_nvl(e.soj, 'es')) = 1 AND cast(sojlib.soj_nvl(e.soj, 'es') AS DECIMAL(4, 0)) <> e.site_id
            THEN sojlib.soj_nvl(e.soj, 'es')
          ELSE e.site_id
        END AS DECIMAL(4, 0)
      ) AS site_id,
      CAST(
        CASE
          WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'n')) = 32 THEN sojlib.soj_nvl(e.soj, 'n')
          ELSE NULL
        END AS STRING
      ) AS cguid,
      e.page_id,
      CAST(sojlib.soj_nvl(e.soj, 'pnact') AS TINYINT) AS pnact,
      CASE
        WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'ssrid'), 18, 0) = 1
            THEN CAST(sojlib.soj_nvl(e.soj, 'ssrid') AS STRING)
        WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'nid'), 18, 0) = 1
            THEN CAST(sojlib.soj_nvl(e.soj, 'nid') AS STRING)
        WHEN sojlib.is_decimal(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'nid'), '%'),',',1), 18, 0) = 1
            THEN CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'nid'), '%') AS STRING)
        ELSE NULL
      END AS notification_id,
      CAST(sojlib.soj_nvl(e.soj, 'mav') AS STRING) AS app_version,
      CAST(sojlib.soj_nvl(e.soj, 'osv') AS STRING) AS os_version,
      CAST(
        CASE
          WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1 THEN sojlib.soj_nvl(e.soj, 'app')
          ELSE NULL
        END AS INTEGER
      ) AS app_id,
      CAST(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'user_name'), '%'),'%') AS STRING) AS user_name,
      CASE
        WHEN COALESCE(sojlib.soj_nvl(e.soj, 'ntype'), sojlib.soj_nvl(e.soj, 'evt')) IN ('GENERIC', 'GENERIC_EVENT')
            THEN NULL
        ELSE CAST(COALESCE(sojlib.soj_nvl(e.soj, 'ntype'), sojlib.soj_nvl(e.soj, 'evt')) AS STRING)
      END AS event_type,
      CASE
        WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'ssrid'), 18, 0) = 1 THEN CAST(sojlib.soj_nvl(e.soj, 'ssrid') AS DECIMAL(18, 0))
        WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'nid'), 18, 0) = 1 THEN CAST(sojlib.soj_nvl(e.soj, 'nid') AS DECIMAL(18, 0))
        WHEN sojlib.is_decimal(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'nid'), '%'),',',1),18,0) = 1
            THEN CAST(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'nid'), '%'),',',1) AS DECIMAL(18, 0))
        ELSE NULL
      END AS frst_nid,
      CAST(NULL AS DECIMAL(18, 0)) AS user_id,
      CAST(NULL AS STRING) AS event_type2,
      CAST(NULL AS STRING) AS app_name,
      CASE
        WHEN sojlib.soj_nvl(soj, 'uep_tracking_id') IS NULL THEN ''
        ELSE 'trkid=' || CAST(sojlib.soj_nvl(soj, 'uep_tracking_id') AS STRING)
      END || CASE
        WHEN sojlib.soj_nvl(soj, 'isUEP') IS NULL THEN ''
        ELSE '&isuep=' || CAST(sojlib.soj_nvl(soj, 'isUEP') AS STRING)
      END || CASE
        WHEN sojlib.soj_nvl(soj, 'recoId') IS NULL THEN ''
        ELSE '&recoid=' || CAST(sojlib.soj_nvl(soj, 'recoId') AS STRING)
      END || CASE
        WHEN sojlib.soj_nvl(soj, 'mesgId') IS NULL THEN ''
        ELSE '&mesgid=' || CAST(sojlib.soj_nvl(soj, 'mesgId') AS STRING)
      END || CASE
        WHEN sojlib.soj_nvl(soj, 'plmtId') IS NULL THEN ''
        ELSE '&plmtid=' || CAST(sojlib.soj_nvl(soj, 'plmtId') AS STRING)
      END as utp_src_string
    FROM
      ubi_v.ubi_event e
    WHERE
      e.session_start_dt BETWEEN '2024-08-12' AND '2024-08-13'
      AND e.guid NOT IN (
        '',
        '52bff6c818a0acd9adc5b7c2fffffdb8',
        '9836df7318e930c36d8aa21001d81925',
        'f946fbac1900a54972862765fffc1dad',
        '821916ce17917022a406aea001b7b5a3',
        '4c3d3e861910a769dba70a12ffffd93b',
        '79d90f461768552109675d9001fcf788',
        '04ab4de61910ab3aff0aa946fe55da56',
        '7a837edc18e0aaf6088a8f7efffd3141',
        '09f5d7a01812015f7c3c0d200161ee1c',
        '7f73afd1182c77267646566001a48d8b',
        '6ed05cdc18d0acd89bfe71bffff5641a',
        'c2ca293c18e3250f84b19360012d825a',
        'a9c73e6f176ba7cef89ba58001db51b6',
        '601029f617cdc70fa4c5ed1001c68d14'
      )
      /* Remove skewed GUIDs from the query to cut down on long runtimes */
      AND e.page_id IN (2054081, 2054060, 2046774)
      /* Notification Actions */
  ) e1
WHERE
  ((e1.session_start_dt < '2018-05-22' AND e1.app_id IS NOT NULL) OR e1.session_start_dt >= '2018-05-22')
  /* modified go-forward for Chrome Browser Notifications */
  AND COALESCE(e1.event_type, 'OTHER') NOT IN ('INTERNAL_BADGE')
  /* Not Valid Events */
  AND (e1.notification_id is NOT NULL or LENGTH(utp_src_string) > 0);

UPDATE t5a
FROM
  p_soj_cl_t.temp_pre2_5a_v1 t5a,
  (
    SELECT
      us.user_slctd_id,
      MAX(us.user_id) AS newest_uid
    FROM
      prs_secure_v.dw_users us
      INNER JOIN p_soj_cl_t.temp_pre2_5a_v1 us2 ON (us.user_slctd_id = us2.user_name)
      GROUP BY 1
  ) u
SET
	t5a.user_id = u.newest_uid
WHERE
  t5a.user_name = u.user_slctd_id;

UPDATE t5a
FROM
  p_soj_cl_t.temp_pre2_5a_v1 t5a,
  (
    SELECT
      notification_id,
      event_type,
      app_name
    FROM
      p_soj_cl_t.notification_sends
    WHERE
      event_dt BETWEEN '2024-08-03' AND '2024-08-13'
      AND notification_id IS NOT NULL
      GROUP BY 1, 2, 3
  ) n
SET
	t5a.event_type2 = n.event_type,
	t5a.app_name = n.app_name
WHERE
  t5a.frst_nid = n.notification_id;

UPDATE t5a
FROM
  p_soj_cl_t.temp_pre2_5a_v1 t5a,
  (
    SELECT
      tracking_id,
      event_type,
      app_name
    FROM
      p_soj_cl_t.notification_sends
    WHERE
      event_dt BETWEEN '2024-08-03' AND '2024-08-13'
      AND tracking_id IS NOT NULL
      GROUP BY 1, 2, 3
  ) n
SET
	t5a.event_type2 = n.event_type,
	t5a.app_name = n.app_name
WHERE
  sojlib.soj_nvl(t5a.utp_src_string, 'trkid') = n.tracking_id;

INSERT INTO p_soj_cl_t.notification_event_dtl
(
    guid,
    session_start_dt,
    session_skey,
    seqnum,
    event_timestamp,
    site_id,
    cguid,
    page_id,
    pnact,
    notification_id,
    app_version,
    os_version,
    app_id,
    user_name,
    event_type,
    frst_nid,
    user_id,
    event_type2,
    app_name,
    utp_src_string,
    updated_ts
  )
SELECT
  guid,
  session_start_dt,
  session_skey,
  seqnum,
  event_timestamp,
  site_id,
  cguid,
  page_id,
  pnact,
  notification_id,
  app_version,
  os_version,
  app_id,
  user_name,
  event_type,
  frst_nid,
  user_id,
  event_type2,
  app_name,
  utp_src_string,
  CURRENT_TIMESTAMP
FROM
  p_soj_cl_t.temp_pre2_5a_v1;