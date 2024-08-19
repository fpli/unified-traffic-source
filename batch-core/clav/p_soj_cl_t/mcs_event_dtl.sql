CREATE TABLE p_soj_cl_t.temp_pre2_11a_v1 USING PARQUET AS
SELECT
  m.*,
  CAST(sojlib.soj_get_url_domain(referrer) AS STRING) AS ref_domain,
  CAST(NULL AS SMALLINT) AS mpx_chnl_id
FROM
  (
    SELECT
      e.guid,
      e.session_skey,
      e.seqnum,
      e.event_timestamp,
      e.site_id,
      e.session_start_dt,
      CAST(
        CASE
          WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'n')) = 32 THEN sojlib.soj_nvl(e.soj, 'n')
          ELSE NULL
        END AS STRING
      ) AS cguid,
      CAST(
        CASE
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%;iPad%' THEN 2878
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayAndroid;%' THEN 2571
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayiPad/%' THEN 2878
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayiPhone/%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayAndroid/%' THEN 2571
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'iphone/5%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'Android/5%' THEN 2571
          WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1 THEN sojlib.soj_nvl(e.soj, 'app')
          /* app logged, might not match session */
          ELSE NULL
        END AS INTEGER
      ) AS app_id,
      e.page_id,
      CAST(sojlib.soj_get_url_domain(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'), '%')) AS STRING) AS url_domain,
      CAST(sojlib.soj_get_url_path(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'), '%')) AS STRING) AS url_path,
      CAST(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'),'%')) AS STRING) AS url_params,
      CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%') AS STRING) referrer,
      CAST(
        CASE
          WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'chnl')) = 1
                THEN sojlib.soj_nvl(e.soj, 'chnl')
          WHEN sojlib.is_integer(sojlib.soj_nvl(url_params, 'mkcid')) = 1
                THEN sojlib.soj_nvl(url_params, 'mkcid')
          ELSE NULL
        END AS INTEGER
      ) AS channel,
      CAST(COALESCE(sojlib.soj_nvl(e.soj, 'gclid'),sojlib.soj_nvl(url_params, 'gclid')) AS STRING) AS gclid,
      CAST(
        CASE
          WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'rvrid'), 18, 0) = 1 THEN sojlib.soj_nvl(e.soj, 'rvrid')
          ELSE NULL
        END AS DECIMAL(18, 0)
      ) AS rover_id,
      CAST(
        CASE
          WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'rotid'), 18, 0) = 1 THEN sojlib.soj_nvl(e.soj, 'rotid')
          ELSE NULL
        END AS DECIMAL(18, 0)
      ) AS rotation_id,
      CAST(
            't=' || date_format(CAST(CAST(e.event_timestamp as timestamp) as STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
            || '&sn=' || CAST(e.seqnum AS STRING)
            || CASE
                    WHEN rover_id IS NULL THEN ''
                    ELSE '&rvr=' || sojlib.soj_replace_char(CAST(rover_id AS STRING), '\\.', '')
               END
            || CASE
                    WHEN rotation_id IS NULL THEN ''
                    ELSE '&rot=' || sojlib.soj_replace_char(CAST(rotation_id AS STRING), '\\.', '')
                END
            || CAST(
                  CASE
                    WHEN sojlib.soj_nvl(e.soj, 'imprvrid') IS NULL OR sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'imprvrid'), 18, 0) = 0 THEN ''
                    ELSE '&imprvrid=' || CAST(sojlib.soj_nvl(e.soj, 'imprvrid') AS STRING)
                  END AS STRING)
            || CASE
                  WHEN COALESCE(sojlib.soj_nvl(e.soj, 'smsid'), sojlib.soj_nvl(url_params, 'smsid')) IS NOT NULL
                        THEN '&smsid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'smsid'),sojlib.soj_nvl(url_params, 'smsid')) AS STRING)
                  ELSE ''
               END
            || CASE
                  WHEN COALESCE(sojlib.soj_nvl(e.soj, 'bannercid'), sojlib.soj_nvl(url_params, 'bannercid')) IS NOT NULL
                        THEN '&bannercid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'bannercid'), sojlib.soj_nvl(url_params, 'bannercid')) AS STRING)
                  ELSE ''
               END
            || CASE
                  WHEN COALESCE(sojlib.soj_nvl(e.soj, 'bannerrid'), sojlib.soj_nvl(url_params, 'bannerrid')) IS NOT NULL
                        THEN '&bannerrid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'bannerrid'),sojlib.soj_nvl(url_params, 'bannerrid')) AS STRING)
                  ELSE ''
               END
            || CASE
                  WHEN COALESCE(sojlib.soj_nvl(e.soj, 'did'), sojlib.soj_nvl(url_params, 'did')) IS NOT NULL
                        THEN '&did=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'did'), sojlib.soj_nvl(url_params, 'did')) AS STRING)
                  ELSE ''
                END
            || CASE
                  WHEN sojlib.soj_nvl(e.soj, 'trkid') is NOT NULL
                    THEN '&trkid=' || CAST(lower(sojlib.soj_nvl(e.soj, 'trkid')) AS STRING)
                  WHEN sojlib.soj_nvl(lower(url_params), 'trkid') is NOT NULL
                    THEN '&trkid=' || CAST(sojlib.soj_nvl(lower(url_params), 'trkid') AS STRING)
                  ELSE ''
               END
            || CASE
                    WHEN sojlib.soj_nvl(e.soj, 'utpid') is NOT NULL
                        THEN '&utpid=' || CAST(lower(sojlib.soj_nvl(e.soj, 'utpid')) AS STRING)
                    WHEN sojlib.soj_nvl(lower(url_params), 'utpid') is NOT NULL
                        THEN '&utpid=' || CAST(sojlib.soj_nvl(lower(url_params), 'utpid') AS STRING)
                    ELSE ''
               END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'mktype') is NOT NULL
                        THEN '&mktype=' || CAST(lower(sojlib.soj_nvl(lower(url_params), 'mktype')) AS STRING)
                    ELSE ''
                END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'media') is NOT NULL
                        THEN '&media=' || CAST(lower(sojlib.soj_nvl(lower(url_params), 'media')) AS STRING)
                    ELSE ''
               END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'mesgid') is NOT NULL
                        THEN '&mesgid=' || CAST(sojlib.soj_nvl(lower(url_params), 'mesgid') AS STRING)
                    ELSE ''
                END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'plmtid') is NOT NULL
                        THEN '&plmtid=' || CAST(sojlib.soj_nvl(lower(url_params), 'plmtid') AS STRING)
                    ELSE ''
               END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'recoid') is NOT NULL
                        THEN '&recoid=' || CAST(sojlib.soj_nvl(lower(url_params), 'recoid') AS STRING)
                    ELSE ''
               END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'recopos') is NOT NULL
                        THEN '&recopos=' || CAST(sojlib.soj_nvl(lower(url_params), 'recopos') AS STRING)
                    ELSE ''
                END
            || CASE
                    WHEN sojlib.soj_nvl(lower(url_params), 'cnvid') is NOT NULL
                        THEN '&cnvid=' || CAST(sojlib.soj_nvl(lower(url_params), 'cnvid') AS STRING)
                    ELSE ''
                END
            || CASE
                    WHEN channel IN (7, 8)
                        THEN CAST(CASE
                                      WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'euid')) = 32 THEN '&euid=' || sojlib.soj_nvl(e.soj, 'euid')
                                      WHEN CHAR_LENGTH(sojlib.soj_nvl(url_params, 'euid')) = 32 THEN '&euid=' || sojlib.soj_nvl(url_params, 'euid')
                                      ELSE ''
                                    END AS STRING)
                               || CASE
                                      WHEN sojlib.is_bigint(COALESCE(sojlib.soj_nvl(e.soj, 'emid'), sojlib.soj_nvl(url_params, 'emid'))) = 1
                                        THEN '&emid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'emid'), sojlib.soj_nvl(url_params, 'emid')) AS STRING)
                                      ELSE ''
                                   END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'email'), sojlib.soj_nvl(url_params, 'email')) IS NOT NULL
                                        THEN '&email=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'email'), sojlib.soj_nvl(url_params, 'email')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'), sojlib.soj_nvl(url_params, 'emsid')), '%'), '%'), '%') LIKE 'e%.%'
                                         THEN '&e=' || CAST(SUBSTR(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'), sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',1), 2, 9999) AS STRING)
                                      WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%') LIKE 'e%'
                                         THEN '&e=' || CAST(SUBSTR(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),2,9999) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%') LIKE '%.m%.%'
                                         THEN CASE
                                                    WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',2) LIKE 'm%'
                                                        THEN '&m=' || SUBSTR(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',2),2,7)
                                                    WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',3) LIKE 'm%'
                                                        THEN '&m=' || SUBSTR(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',3),2,7)
                                                    ELSE ''
                                              END
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%') LIKE '%.l%'
                                         THEN CASE
                                                    WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',3) LIKE 'l%'
                                                        THEN '&l=' || SUBSTR(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',3),2,7)
                                                    WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'),sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.', 4) LIKE 'l%'
                                                        THEN '&l=' || SUBSTR(sojlib.soj_list_get_val_by_idx(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(COALESCE(sojlib.soj_nvl(e.soj, 'emsid'), sojlib.soj_nvl(url_params, 'emsid')),'%'),'%'),'%'),'\\.',4),2,7)
                                                    ELSE ''
                                              END
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN sojlib.is_bigint(COALESCE(sojlib.soj_nvl(e.soj, 'ext'), sojlib.soj_nvl(url_params, 'ext'))) = 1
                                         THEN '&ext=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'ext'), sojlib.soj_nvl(url_params, 'ext')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN CHAR_LENGTH(COALESCE(sojlib.soj_nvl(e.soj, 'crd'), sojlib.soj_nvl(url_params, 'crd'))) = 14
                                         THEN '&crd=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'crd'),sojlib.soj_nvl(url_params, 'crd')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'segname'), sojlib.soj_nvl(url_params, 'segname')) IS NOT NULL
                                         THEN '&segname=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'segname'),sojlib.soj_nvl(url_params, 'segname')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'co'), sojlib.soj_nvl(url_params, 'co')) IS NOT NULL
                                         THEN '&co=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'co'), sojlib.soj_nvl(url_params, 'co')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'ymmmid'), sojlib.soj_nvl(url_params, 'ymmmid')) IS NOT NULL
                                          THEN '&ymmmid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'ymmmid'), sojlib.soj_nvl(url_params, 'ymmmid')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'ymsid'), sojlib.soj_nvl(url_params, 'ymsid')) IS NOT NULL
                                          THEN '&ymsid=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'ymsid'),sojlib.soj_nvl(url_params, 'ymsid')) AS STRING)
                                      ELSE ''
                                  END
                               || CASE
                                      WHEN COALESCE(sojlib.soj_nvl(e.soj, 'yminstc'), sojlib.soj_nvl(url_params, 'yminstc')) IS NOT NULL
                                          THEN '&yminstc=' || CAST(COALESCE(sojlib.soj_nvl(e.soj, 'yminstc'), sojlib.soj_nvl(url_params, 'yminstc')) AS STRING)
                                      ELSE ''
                                  END
                    ELSE ''
               END
       AS STRING) AS src_string,
      CAST(
        replace(
          CASE
            WHEN sojlib.soj_nvl(e.soj, 'keyword') IS NOT NULL
                THEN sojlib.soj_nvl(e.soj, 'keyword')
            WHEN url_params LIKE '%keyword=%'
                THEN sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_params, 'keyword'), '%'), '+', ' '))
            WHEN url_params LIKE '%rawquery=%'
                THEN sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_params, 'rawquery'), '%'),'+',' '))
            ELSE NULL
          END,
          '&',
          '%26'
        ) AS STRING
      ) AS bought_keyword,
      CAST(COALESCE(sojlib.soj_nvl(url_params, 'campid'), sojlib.soj_nvl(url_params, 'amp;campid')) AS STRING) AS campaign_id,
      CAST(sojlib.soj_nvl(url_params, 'crlp') AS STRING) AS crlp,
      CAST(sojlib.soj_nvl(url_params, 'geo_id') AS STRING) AS geo_id
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
      AND e.page_id IN (2547208)
      /* New Rover Migration Event */
      AND e.site_id IS NOT NULL
  ) m;

CREATE TABLE p_soj_cl_t.temp_pre2_11b_v1 USING PARQUET AS
SELECT
  m.*,
  CAST(sojlib.soj_get_url_domain(referrer) AS STRING) AS ref_domain,
  CAST(NULL AS SMALLINT) AS mpx_chnl_id
FROM
  (
    SELECT
      e.guid,
      e.session_skey,
      e.seqnum,
      e.event_timestamp,
      e.site_id,
      e.session_start_dt,
      CAST(
        CASE
          WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'n')) = 32 THEN sojlib.soj_nvl(e.soj, 'n')
          ELSE NULL
        END AS STRING
      ) AS cguid,
      CAST(
        CASE
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%;iPad%' THEN 2878
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayIOS;%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'ebayUserAgent/eBayAndroid;%' THEN 2571
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayiPad/%' THEN 2878
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayiPhone/%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'eBayAndroid/%' THEN 2571
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'iphone/5%' THEN 1462
          WHEN sojlib.soj_parse_clientinfo(e.client_data, 'Agent') LIKE 'Android/5%' THEN 2571
          WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'app')) = 1 THEN sojlib.soj_nvl(e.soj, 'app')
          /* app logged, might not match session */
          ELSE NULL
        END AS INTEGER
      ) AS app_id,
      e.page_id,
      CAST(
        sojlib.soj_get_url_domain(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'),
            '%'
          )
        ) AS STRING
      ) AS url_domain,
      CAST(
        sojlib.soj_get_url_path(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'),
            '%'
          )
        ) AS STRING
      ) AS url_path,
      CAST(
        sojlib.soj_get_url_params(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'url_mpre'), '%'),
            '%'
          )
        ) AS STRING
      ) AS url_params,
      CAST(
        sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%') AS STRING
      ) referrer,
      CAST(
        CASE
          WHEN sojlib.is_integer(sojlib.soj_nvl(e.soj, 'chnl')) = 1 THEN sojlib.soj_nvl(e.soj, 'chnl')
          WHEN sojlib.is_integer(sojlib.soj_nvl(url_params, 'mkcid')) = 1 THEN sojlib.soj_nvl(url_params, 'mkcid')
          ELSE NULL
        END AS INTEGER
      ) AS channel,
      CAST(
        COALESCE(
          sojlib.soj_nvl(e.soj, 'gclid'),
          sojlib.soj_nvl(url_params, 'gclid')
        ) AS STRING
      ) AS gclid,
      CAST(
        CASE
          WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'rvrid'), 18, 0) = 1 THEN sojlib.soj_nvl(e.soj, 'rvrid')
          ELSE NULL
        END AS DECIMAL(18, 0)
      ) AS rover_id,
      CAST(
        CASE
          WHEN sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'rotid'), 18, 0) = 1 THEN sojlib.soj_nvl(e.soj, 'rotid')
          ELSE NULL
        END AS DECIMAL(18, 0)
      ) AS rotation_id,
      CAST(
        't=' || date_format(
          CAST(CAST(e.event_timestamp as timestamp) as STRING),
          'yyyy-MM-dd HH:mm:ss.SSS'
        ) || '&sn=' || CAST(e.seqnum AS STRING) || CASE
          WHEN rover_id IS NULL THEN ''
          ELSE '&rvr=' || sojlib.soj_replace_char(CAST(rover_id AS STRING), '\\.', '')
        END || CASE
          WHEN rotation_id IS NULL THEN ''
          ELSE '&rot=' || sojlib.soj_replace_char(CAST(rotation_id AS STRING), '\\.', '')
        END || CAST(
          CASE
            WHEN sojlib.soj_nvl(e.soj, 'imprvrid') IS NULL OR sojlib.is_decimal(sojlib.soj_nvl(e.soj, 'imprvrid'), 18, 0) = 0 THEN ''
            ELSE '&imprvrid=' || CAST(sojlib.soj_nvl(e.soj, 'imprvrid') AS STRING)
          END AS STRING
        ) || CASE
          WHEN COALESCE(
            sojlib.soj_nvl(e.soj, 'smsid'),
            sojlib.soj_nvl(url_params, 'smsid')
          ) IS NOT NULL THEN '&smsid=' || CAST(
            COALESCE(
              sojlib.soj_nvl(e.soj, 'smsid'),
              sojlib.soj_nvl(url_params, 'smsid')
            ) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN COALESCE(
            sojlib.soj_nvl(e.soj, 'bannercid'),
            sojlib.soj_nvl(url_params, 'bannercid')
          ) IS NOT NULL THEN '&bannercid=' || CAST(
            COALESCE(
              sojlib.soj_nvl(e.soj, 'bannercid'),
              sojlib.soj_nvl(url_params, 'bannercid')
            ) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN COALESCE(
            sojlib.soj_nvl(e.soj, 'bannerrid'),
            sojlib.soj_nvl(url_params, 'bannerrid')
          ) IS NOT NULL THEN '&bannerrid=' || CAST(
            COALESCE(
              sojlib.soj_nvl(e.soj, 'bannerrid'),
              sojlib.soj_nvl(url_params, 'bannerrid')
            ) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN COALESCE(
            sojlib.soj_nvl(e.soj, 'did'),
            sojlib.soj_nvl(url_params, 'did')
          ) IS NOT NULL THEN '&did=' || CAST(
            COALESCE(
              sojlib.soj_nvl(e.soj, 'did'),
              sojlib.soj_nvl(url_params, 'did')
            ) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(e.soj, 'trkid') is NOT NULL THEN '&trkid=' || CAST(lower(sojlib.soj_nvl(e.soj, 'trkid')) AS STRING)
          WHEN sojlib.soj_nvl(lower(url_params), 'trkid') is NOT NULL THEN '&trkid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'trkid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(e.soj, 'utpid') is NOT NULL THEN '&utpid=' || CAST(lower(sojlib.soj_nvl(e.soj, 'utpid')) AS STRING)
          WHEN sojlib.soj_nvl(lower(url_params), 'utpid') is NOT NULL THEN '&utpid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'utpid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'mktype') is NOT NULL THEN '&mktype=' || CAST(
            lower(sojlib.soj_nvl(lower(url_params), 'mktype')) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'media') is NOT NULL THEN '&media=' || CAST(
            lower(sojlib.soj_nvl(lower(url_params), 'media')) AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'mesgid') is NOT NULL THEN '&mesgid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'mesgid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'plmtid') is NOT NULL THEN '&plmtid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'plmtid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'recoid') is NOT NULL THEN '&recoid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'recoid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'recopos') is NOT NULL THEN '&recopos=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'recopos') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN sojlib.soj_nvl(lower(url_params), 'cnvid') is NOT NULL THEN '&cnvid=' || CAST(
            sojlib.soj_nvl(lower(url_params), 'cnvid') AS STRING
          )
          ELSE ''
        END || CASE
          WHEN channel IN (7, 8) THEN CAST(
            CASE
              WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'euid')) = 32 THEN '&euid=' || sojlib.soj_nvl(e.soj, 'euid')
              WHEN CHAR_LENGTH(sojlib.soj_nvl(url_params, 'euid')) = 32 THEN '&euid=' || sojlib.soj_nvl(url_params, 'euid')
              ELSE ''
            END AS STRING
          ) || CASE
            WHEN sojlib.is_bigint(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'emid'),
                sojlib.soj_nvl(url_params, 'emid')
              )
            ) = 1 THEN '&emid=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'emid'),
                sojlib.soj_nvl(url_params, 'emid')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'email'),
              sojlib.soj_nvl(url_params, 'email')
            ) IS NOT NULL THEN '&email=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'email'),
                sojlib.soj_nvl(url_params, 'email')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN sojlib.soj_url_decode_escapes(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  COALESCE(
                    sojlib.soj_nvl(e.soj, 'emsid'),
                    sojlib.soj_nvl(url_params, 'emsid')
                  ),
                  '%'
                ),
                '%'
              ),
              '%'
            ) LIKE 'e%.%' THEN '&e=' || CAST(
              SUBSTR(
                sojlib.soj_list_get_val_by_idx(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_url_decode_escapes(
                        COALESCE(
                          sojlib.soj_nvl(e.soj, 'emsid'),
                          sojlib.soj_nvl(url_params, 'emsid')
                        ),
                        '%'
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '\\.',
                  1
                ),
                2,
                9999
              ) AS STRING
            )
            WHEN sojlib.soj_url_decode_escapes(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  COALESCE(
                    sojlib.soj_nvl(e.soj, 'emsid'),
                    sojlib.soj_nvl(url_params, 'emsid')
                  ),
                  '%'
                ),
                '%'
              ),
              '%'
            ) LIKE 'e%' THEN '&e=' || CAST(
              SUBSTR(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      COALESCE(
                        sojlib.soj_nvl(e.soj, 'emsid'),
                        sojlib.soj_nvl(url_params, 'emsid')
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '%'
                ),
                2,
                9999
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN sojlib.soj_url_decode_escapes(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  COALESCE(
                    sojlib.soj_nvl(e.soj, 'emsid'),
                    sojlib.soj_nvl(url_params, 'emsid')
                  ),
                  '%'
                ),
                '%'
              ),
              '%'
            ) LIKE '%.m%.%' THEN CASE
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      COALESCE(
                        sojlib.soj_nvl(e.soj, 'emsid'),
                        sojlib.soj_nvl(url_params, 'emsid')
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '%'
                ),
                '\\.',
                2
              ) LIKE 'm%' THEN '&m=' || SUBSTR(
                sojlib.soj_list_get_val_by_idx(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_url_decode_escapes(
                        COALESCE(
                          sojlib.soj_nvl(e.soj, 'emsid'),
                          sojlib.soj_nvl(url_params, 'emsid')
                        ),
                        '%'
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '\\.',
                  2
                ),
                2,
                7
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      COALESCE(
                        sojlib.soj_nvl(e.soj, 'emsid'),
                        sojlib.soj_nvl(url_params, 'emsid')
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '%'
                ),
                '\\.',
                3
              ) LIKE 'm%' THEN '&m=' || SUBSTR(
                sojlib.soj_list_get_val_by_idx(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_url_decode_escapes(
                        COALESCE(
                          sojlib.soj_nvl(e.soj, 'emsid'),
                          sojlib.soj_nvl(url_params, 'emsid')
                        ),
                        '%'
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '\\.',
                  3
                ),
                2,
                7
              )
              ELSE ''
            END
            ELSE ''
          END || CASE
            WHEN sojlib.soj_url_decode_escapes(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  COALESCE(
                    sojlib.soj_nvl(e.soj, 'emsid'),
                    sojlib.soj_nvl(url_params, 'emsid')
                  ),
                  '%'
                ),
                '%'
              ),
              '%'
            ) LIKE '%.l%' THEN CASE
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      COALESCE(
                        sojlib.soj_nvl(e.soj, 'emsid'),
                        sojlib.soj_nvl(url_params, 'emsid')
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '%'
                ),
                '\\.',
                3
              ) LIKE 'l%' THEN '&l=' || SUBSTR(
                sojlib.soj_list_get_val_by_idx(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_url_decode_escapes(
                        COALESCE(
                          sojlib.soj_nvl(e.soj, 'emsid'),
                          sojlib.soj_nvl(url_params, 'emsid')
                        ),
                        '%'
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '\\.',
                  3
                ),
                2,
                7
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      COALESCE(
                        sojlib.soj_nvl(e.soj, 'emsid'),
                        sojlib.soj_nvl(url_params, 'emsid')
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '%'
                ),
                '\\.',
                4
              ) LIKE 'l%' THEN '&l=' || SUBSTR(
                sojlib.soj_list_get_val_by_idx(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_url_decode_escapes(
                        COALESCE(
                          sojlib.soj_nvl(e.soj, 'emsid'),
                          sojlib.soj_nvl(url_params, 'emsid')
                        ),
                        '%'
                      ),
                      '%'
                    ),
                    '%'
                  ),
                  '\\.',
                  4
                ),
                2,
                7
              )
              ELSE ''
            END
            ELSE ''
          END || CASE
            WHEN sojlib.is_bigint(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'ext'),
                sojlib.soj_nvl(url_params, 'ext')
              )
            ) = 1 THEN '&ext=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'ext'),
                sojlib.soj_nvl(url_params, 'ext')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN CHAR_LENGTH(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'crd'),
                sojlib.soj_nvl(url_params, 'crd')
              )
            ) = 14 THEN '&crd=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'crd'),
                sojlib.soj_nvl(url_params, 'crd')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'segname'),
              sojlib.soj_nvl(url_params, 'segname')
            ) IS NOT NULL THEN '&segname=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'segname'),
                sojlib.soj_nvl(url_params, 'segname')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'co'),
              sojlib.soj_nvl(url_params, 'co')
            ) IS NOT NULL THEN '&co=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'co'),
                sojlib.soj_nvl(url_params, 'co')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'ymmmid'),
              sojlib.soj_nvl(url_params, 'ymmmid')
            ) IS NOT NULL THEN '&ymmmid=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'ymmmid'),
                sojlib.soj_nvl(url_params, 'ymmmid')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'ymsid'),
              sojlib.soj_nvl(url_params, 'ymsid')
            ) IS NOT NULL THEN '&ymsid=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'ymsid'),
                sojlib.soj_nvl(url_params, 'ymsid')
              ) AS STRING
            )
            ELSE ''
          END || CASE
            WHEN COALESCE(
              sojlib.soj_nvl(e.soj, 'yminstc'),
              sojlib.soj_nvl(url_params, 'yminstc')
            ) IS NOT NULL THEN '&yminstc=' || CAST(
              COALESCE(
                sojlib.soj_nvl(e.soj, 'yminstc'),
                sojlib.soj_nvl(url_params, 'yminstc')
              ) AS STRING
            )
            ELSE ''
          END
          ELSE ''
        END AS STRING
      ) AS src_string,
      CAST(
        replace(
          CASE
            WHEN sojlib.soj_nvl(e.soj, 'keyword') IS NOT NULL THEN sojlib.soj_nvl(e.soj, 'keyword')
            WHEN url_params LIKE '%keyword=%' THEN sojlib.soj_collapse_whitespace(
              sojlib.soj_replace_char(
                sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_params, 'keyword'), '%'),
                '+',
                ' '
              )
            )
            WHEN url_params LIKE '%rawquery=%' THEN sojlib.soj_collapse_whitespace(
              sojlib.soj_replace_char(
                sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_params, 'rawquery'), '%'),
                '+',
                ' '
              )
            )
            ELSE NULL
          END,
          '&',
          '%26'
        ) AS STRING
      ) AS bought_keyword,
      CAST(
        COALESCE(
          sojlib.soj_nvl(url_params, 'campid'),
          sojlib.soj_nvl(url_params, 'amp;campid')
        ) AS STRING
      ) AS campaign_id,
      CAST(sojlib.soj_nvl(url_params, 'crlp') AS STRING) AS crlp,
      CAST(sojlib.soj_nvl(url_params, 'geo_id') AS STRING) AS geo_id
    FROM
      ubi_v.ubi_event_skew e
    WHERE
      e.session_start_dt BETWEEN '2024-08-12'
      AND '2024-08-13'
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
      AND e.page_id IN (2547208)
      /* New Rover Migration Event */
      AND e.site_id IS NOT NULL
  ) m;

INSERT INTO
  p_soj_cl_t.temp_pre2_11a_v1
SELECT
  b.*
FROM
  p_soj_cl_t.temp_pre2_11b_v1 b;

UPDATE t11a
FROM
  p_soj_cl_t.temp_pre2_11a_v1 t11a,
  choco_data_v.dw_mpx_rotations r
SET t11a.mpx_chnl_id = CASE
    						WHEN r.campaign_id = 137245
								THEN 97
    					    ELSE r.mpx_chnl_id
                        END
WHERE
  t11a.rotation_id = r.rotation_id
  AND t11a.rotation_id IS NOT NULL;

UPDATE t11a
FROM
  p_soj_cl_t.temp_pre2_11a_v1 t11a,
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
	t11a.src_string = case
    						when t11a.src_string is null
								then ''
    						else t11a.src_string
  					  end || '&epn_campid=' || CAST(t11a.campaign_id AS STRING) || '&epn_group=' || CAST(COALESCE(r.partner_group, 'unmapped') AS STRING)
WHERE
  r.campaign_id = t11a.campaign_id
  and t11a.mpx_chnl_id = 6
  and t11a.campaign_id is not null;

INSERT INTO p_soj_cl_t.mcs_event_dtl
(
    guid,
    session_skey,
    seqnum,
    event_timestamp,
    site_id,
    session_start_dt,
    cguid,
    app_id,
    page_id,
    url_domain,
    url_path,
    url_params,
    referrer,
    channel,
    gclid,
    rover_id,
    rotation_id,
    src_string,
    bought_keyword,
    campaign_id,
    crlp,
    geo_id,
    ref_domain,
    mpx_chnl_id,
    udpated_ts
)
SELECT
  guid,
  session_skey,
  seqnum,
  event_timestamp,
  site_id,
  session_start_dt,
  cguid,
  app_id,
  page_id,
  url_domain,
  url_path,
  url_params,
  referrer,
  channel,
  gclid,
  rover_id,
  rotation_id,
  CASE
    WHEN src_string IS NULL
        THEN "mcs_event_key=guid:" || guid || "|sskey:" || CAST(session_skey AS STRING) || "|seq:" || CAST(seqnum AS STRING) || "|ts:" || date_format(CAST(CAST(event_timestamp AS TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
    ELSE src_string || "&mcs_event_key=guid:" || guid || "|sskey:" || CAST(session_skey AS STRING) || "|seq:" || CAST(seqnum AS STRING) || "|ts:" || date_format(CAST(CAST(event_timestamp AS TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS')
  END AS src_string,
  bought_keyword,
  campaign_id,
  crlp,
  geo_id,
  ref_domain,
  mpx_chnl_id,
  CURRENT_TIMESTAMP
FROM
  p_soj_cl_t.temp_pre2_11a_v1;