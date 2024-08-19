DROP TABLE IF EXISTS p_soj_cl_t.notsend_temp1;

CREATE TABLE p_soj_cl_t.notsend_temp1 USING delta AS
SELECT
  coalesce(
    sojlib.soj_nvl(trckng_txt, 'tracking.id'),
    sojlib.soj_nvl(trckng_txt, 'trackingId'),
    CASE
      WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'nid'), 18, 6) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'nid')
      ELSE NULL
    END
  ) as global_tracking_id,
  CASE
    WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'nid'), 18, 6) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'nid')
    ELSE NULL
  END AS notification_id,
  coalesce(
    sojlib.soj_nvl(trckng_txt, 'tracking.id'),
    sojlib.soj_nvl(trckng_txt, 'trackingId')
  ) AS tracking_id,
  CASE
    WHEN sojlib.soj_nvl(trckng_txt, 'eventtype') IS NOT NULL THEN sojlib.soj_nvl(trckng_txt, 'eventtype')
    WHEN sojlib.soj_nvl(trckng_txt, 'send_mesg_id') = 3015 THEN 'PurchaseAssistance_Generic'
    WHEN sojlib.soj_nvl(trckng_txt, 'send_mesg_id') = 3017 THEN 'HOT_ITEM'
    WHEN sojlib.soj_nvl(trckng_txt, 'send_mesg_id') IS NOT NULL THEN 'MessageId_' || sojlib.soj_nvl(trckng_txt, 'send_mesg_id')
    ELSE 'UNKNOWN'
  END AS event_type,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'appname') AS STRING
  ) AS app_name,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'appversion') AS STRING
  ) AS app_version,
  CASE
    WHEN sojlib.soj_nvl(n.trckng_txt, 'deviceid') IS NOT NULL
    AND CAST(
      sojlib.soj_nvl(n.trckng_txt, 'deviceid') RLIKE '[a-f0-9]{32}' AS integer
    ) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'deviceid')
    WHEN sojlib.soj_nvl(n.trckng_txt, 'guid') IS NOT NULL
    AND CAST(
      sojlib.soj_nvl(n.trckng_txt, 'guid') RLIKE '[a-f0-9]{32}' AS integer
    ) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'guid')
    ELSE NULL
  END AS guid,
  CAST(
    sojlib.soj_url_decode_escapes(sojlib.soj_nvl(n.trckng_txt, 'user_name'), '%') AS STRING
  ) AS user_name,
  CAST(
    CASE
      WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'userid'), 18, 0) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'userid')
      ELSE NULL
    END AS DECIMAL(18, 0)
  ) AS user_id,
  CAST(
    CASE
      WHEN sojlib.is_integer(sojlib.soj_nvl(n.trckng_txt, 'p')) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'p')
      ELSE NULL
    END AS INTEGER
  ) AS page_id,
  CAST(
    CASE
      WHEN sojlib.is_integer(sojlib.soj_nvl(n.trckng_txt, 'app')) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'app')
      ELSE NULL
    END AS INTEGER
  ) AS app_id,
  CAST(
    CASE
      WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 't'), 4, 0) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 't')
      ELSE NULL
    END AS DECIMAL(4, 0)
  ) AS site_id,
  n.mts_trckng_rowkey,
  n.event_dt,
  CAST(
    CAST(n.event_dt AS VARCHAR(10)) || ' ' || CAST(n.event_tm AS VARCHAR(8)) AS TIMESTAMP
  ) AS event_timestamp,
  CAST(
    CASE
      WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'pnid'), 18, 0) = 1 THEN sojlib.soj_nvl(n.trckng_txt, 'pnid')
      ELSE NULL
    END AS BIGINT
  ) AS platform_notification_id,
  CASE
    WHEN sojlib.soj_nvl(n.trckng_txt, 'eventtype') IN (
      'WATCHONSALE',
      'CARTONSALE',
      'BUYER_NO_SUCCESS',
      'HOT_ITEM',
      'ITEM_RELISTED',
      'PackageDeliveryConfirmation',
      'ITEM_ON_SALE',
      'WATCHITM',
      'ITMSHPD',
      'BIDRCVD',
      'COCMPLT',
      'ITMSOLD',
      'BIDITEM',
      'OUTBID',
      'ITMWON',
      'BESTOFR',
      'ITMPAID',
      'CNTROFFR',
      'BODECLND',
      'SHOPCARTITM'
    )
    AND sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'refid'), 18, 0) = 1 THEN CAST(
      sojlib.soj_nvl(n.trckng_txt, 'refid') AS DECIMAL(18, 0)
    )
    ELSE NULL
  END AS item_id,
  CASE
    WHEN sojlib.soj_nvl(n.trckng_txt, 'eventtype') IN ('MSGM2MMSGHDR')
    AND sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'refid'), 18, 0) = 1 THEN CAST(
      sojlib.soj_nvl(n.trckng_txt, 'refid') AS DECIMAL(18, 0)
    )
    ELSE NULL
  END AS msg_content_id,
  CASE
    WHEN sojlib.soj_nvl(n.trckng_txt, 'eventtype') IN ('SVDSRCH')
    AND sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'refid'), 18, 0) = 1 THEN CAST(
      sojlib.soj_nvl(n.trckng_txt, 'refid') AS DECIMAL(18, 0)
    )
    ELSE NULL
  END AS saved_search_id,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'exprmnt_id') AS STRING
  ) AS exprmnt_id,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'trtmnt_id') AS STRING
  ) AS trtmnt_id,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'feature_id') AS STRING
  ) AS feature_id,
  CAST(
    CASE
      WHEN sojlib.soj_nvl(n.trckng_txt, 'eventtype') LIKE '%MarketingCampaign%' THEN coalesce(
        sojlib.soj_nvl(n.trckng_txt, 'campaignCode'),
        sojlib.soj_nvl(n.trckng_txt, 'campaign_id')
      )
      ELSE NULL
    END AS STRING
  ) AS campaign_id,
  CAST(
    CASE
      WHEN sojlib.soj_nvl(n.trckng_txt, 'eventtype') IN ('SVDSRCH') THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(n.trckng_txt, 'iid'), '%')
      ELSE NULL
    END AS STRING
  ) AS interest_ids,
  CAST(
    sojlib.soj_nvl(n.trckng_txt, 'pushEspresso') AS STRING
  ) AS espresso_id,
  coalesce(sojlib.soj_nvl(trckng_txt, 'isUEP'), 'false') AS is_uep,
  sojlib.soj_nvl(trckng_txt, 'annotation.cnv.id') canvas_id,
  sojlib.soj_nvl(trckng_txt, 'send_mesg_id') message_id,
  sojlib.soj_nvl(trckng_txt, 'annotation.mesg.list') message_list
FROM
  prs_restricted_v.mbl_mts_tracking n
WHERE
  event_dt BETWEEN '2024-08-12'
  AND '2024-08-13'
  AND n.event_name = 'MobileNotificationEvent'
  AND coalesce(
    sojlib.soj_nvl(trckng_txt, 'tracking.id'),
    sojlib.soj_nvl(trckng_txt, 'trackingId'),
    CASE
      WHEN sojlib.is_decimal(sojlib.soj_nvl(n.trckng_txt, 'nid'), 18, 6) = 1
            THEN sojlib.soj_nvl(n.trckng_txt, 'nid')
      ELSE NULL
    END
  ) is NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29;