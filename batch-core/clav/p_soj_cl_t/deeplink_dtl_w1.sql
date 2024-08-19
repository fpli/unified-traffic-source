DROP TABLE IF EXISTS p_soj_cl_t.temp_pre2_12aa_v1;
CREATE TABLE p_soj_cl_t.temp_pre2_12aa_v1 USING PARQUET AS
SELECT
  d1.*,
  CAST(
    CASE
      WHEN rover_url IS NULL AND sojlib.soj_nvl(sojlib.soj_get_url_params(d1.deeplink), 'mkrid') <> '0'
            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(d1.deeplink), 'mkrid')
      WHEN sojlib.soj_replace_char(sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4), '-', '') = '0'
            THEN NULL
      WHEN sojlib.is_decimal(sojlib.soj_replace_char(sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4), '-', ''), 18,  0) = 1
            THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4)
      ELSE NULL
    END AS STRING
  ) AS rotation_string,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_itm,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_epid,
  CAST(NULL AS STRING) AS dlk_sqr,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_bnid,
  CAST(NULL AS STRING) AS sabg,
  CAST(NULL AS STRING) AS sabc,
  CAST(NULL AS BIGINT) AS sabs,
  CAST(NULL AS STRING) AS dlguid,
  CAST(NULL AS STRING) AS gclid,
  CAST(NULL AS STRING) AS dl_type,
  CAST(NULL AS DECIMAL(18, 0)) AS rvrid,
  CAST(NULL AS INTEGER) AS tracking_partner,
  CAST(NULL AS INTEGER) AS channel,
  CAST(NULL AS STRING) AS campaign_id,
  CAST(NULL AS STRING) AS crlp,
  CAST(NULL AS STRING) AS geo_id,
  CAST(NULL AS STRING) AS bought_keyword,
  CAST(NULL AS SMALLINT) AS mpx_chnl_id,
  CAST(NULL AS DECIMAL(18, 0)) AS rotation_id,
  CAST(NULL AS STRING) AS src_string,
  CAST(NULL AS TINYINT) AS link_src
FROM
  (
    SELECT
      d.*,
      CASE
        WHEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'referrer'), '%') LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%')
            THEN CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'referrer'), '%') AS STRING)
        WHEN sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'ul_ref'), '%'), '%') LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%')
            THEN CAST(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'ul_ref'),'%'),'%') AS STRING)
        WHEN d.deeplink LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%')
            THEN CAST(d.deeplink AS STRING)
        ELSE NULL
      END AS rover_url
    FROM
      (
        SELECT
          guid,
          session_skey,
          site_id,
          session_start_dt,
          seqnum,
          event_timestamp,
          CAST(
            CASE
              WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'n')) = 32 THEN sojlib.soj_nvl(e.soj, 'n')
              ELSE NULL
            END AS STRING
          ) AS cguid,
          user_id,
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
          CAST(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(soj, 'deeplink'), '%'), '%') AS STRING) AS deeplink,
          CAST(
            CASE
              WHEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') IS NOT NULL
                      AND sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') NOT LIKE ALL (
                        '10.%',
                        '172.16.%',
                        '192.168.%',
                        '127.0.0.1',
                        '%,10.%',
                        '%,172.16.%',
                        '%,192.168.%',
                        '%,127.0.0.1%'
                      )
                     THEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP')
              WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) IS NOT NULL
                      AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1) NOT LIKE ALL (
                        '10.%',
                        '172.16.%',
                        '192.168.%',
                        '127.0.0.1',
                        '%,10.%',
                        '%,172.16.%',
                        '%,192.168.%',
                        '%,127.0.0.1%'
                      )
                     THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ',', 1)
              WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) IS NOT NULL
                      AND sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1) NOT LIKE ALL (
                        '10.%',
                        '172.16.%',
                        '192.168.%',
                        '127.0.0.1',
                        '%,10.%',
                        '%,172.16.%',
                        '%,192.168.%',
                        '%,127.0.0.1%'
                      )
                     THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'), ';', 1)
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              )
              ELSE NULL
            END AS STRING
          ) AS external_ip,
          CAST(
            CASE
              WHEN sojlib.soj_nvl(e.soj, 'ref') IS NOT NULL
                      AND sojlib.soj_url_decode_escapes(
                        sojlib.soj_url_decode_escapes(
                          sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%'),
                          '%'
                        ),
                        '%'
                      ) NOT LIKE ALL ('%//rover.%', 'null')
                     THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%')
              WHEN sojlib.soj_nvl(e.soj, 'rurl') IS NOT NULL
                      AND sojlib.soj_url_decode_escapes(
                        sojlib.soj_url_decode_escapes(
                          sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'rurl'), '%'),
                          '%'
                        ),
                        '%'
                      ) NOT LIKE ALL ('%//rover.%', 'null')
                     THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'rurl'), '%')
              WHEN sojlib.soj_nvl(
                        sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                        'dlsource'
                      ) IS NOT NULL
                      AND sojlib.soj_url_decode_escapes(
                        sojlib.soj_nvl(
                          sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                          'dlsource'
                        ),
                        '%'
                      ) NOT LIKE ALL(
                        'google',
                        'ntray',
                        'ninbox',
                        'unknown',
                        'null',
                        '%//rover.%'
                      )
                     THEN sojlib.soj_url_decode_escapes(
                        sojlib.soj_nvl(
                          sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                          'dlsource'
                        ),
                        '%'
                      )
              ELSE NULL
            END AS STRING
          ) AS referrer,
          CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(soj, 'deeplink'), '%'), '%')), '_trkparms'),'%') AS STRING) AS trkparms,
          CAST(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'), 'dlsource'), '%') AS string) AS dlsource
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
          AND e.page_id IN (2367320)
          /* Deep Link Action */
      ) d
  ) d1;

INSERT INTO
  p_soj_cl_t.temp_pre2_12aa_v1
SELECT
  d1.*,
  CAST(
    CASE
      WHEN rover_url IS NULL
      AND sojlib.soj_nvl(sojlib.soj_get_url_params(d1.deeplink), 'mkrid') <> '0' THEN sojlib.soj_nvl(sojlib.soj_get_url_params(d1.deeplink), 'mkrid')
      WHEN sojlib.soj_replace_char(
        sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4),
        '-',
        ''
      ) = '0' THEN NULL
      WHEN sojlib.is_decimal(
        sojlib.soj_replace_char(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4),
          '-',
          ''
        ),
        18,
        0
      ) = 1 THEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(d1.rover_url), '/', 4)
      ELSE NULL
    END AS STRING
  ) AS rotation_string,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_itm,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_epid,
  CAST(NULL AS STRING) AS dlk_sqr,
  CAST(NULL AS DECIMAL(18, 0)) AS dlk_bnid,
  CAST(NULL AS STRING) AS sabg,
  CAST(NULL AS STRING) AS sabc,
  CAST(NULL AS BIGINT) AS sabs,
  CAST(NULL AS STRING) AS dlguid,
  CAST(NULL AS STRING) AS gclid,
  CAST(NULL AS STRING) AS dl_type,
  CAST(NULL AS DECIMAL(18, 0)) AS rvrid,
  CAST(NULL AS INTEGER) AS tracking_partner,
  CAST(NULL AS INTEGER) AS channel,
  CAST(NULL AS STRING) AS campaign_id,
  CAST(NULL AS STRING) AS crlp,
  CAST(NULL AS STRING) AS geo_id,
  CAST(NULL AS STRING) AS bought_keyword,
  CAST(NULL AS SMALLINT) AS mpx_chnl_id,
  CAST(NULL AS DECIMAL(18, 0)) AS rotation_id,
  CAST(NULL AS STRING) AS src_string,
  CAST(NULL AS TINYINT) AS link_src
FROM
  (
    SELECT
      d.*,
      CASE
        WHEN sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'referrer'),
          '%'
        ) LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%') THEN CAST(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'referrer'),
            '%'
          ) AS STRING
        )
        WHEN sojlib.soj_url_decode_escapes(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'ul_ref'),
            '%'
          ),
          '%'
        ) LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%') THEN CAST(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(sojlib.soj_get_url_params(d.deeplink), 'ul_ref'),
              '%'
            ),
            '%'
          ) AS STRING
        )
        WHEN d.deeplink LIKE ANY ('https://rover.ebay%', 'http://rover.ebay%') THEN CAST(d.deeplink AS STRING)
        ELSE NULL
      END AS rover_url
    FROM
      (
        SELECT
          guid,
          session_skey,
          site_id,
          session_start_dt,
          seqnum,
          event_timestamp,
          CAST(
            CASE
              WHEN CHAR_LENGTH(sojlib.soj_nvl(e.soj, 'n')) = 32 THEN sojlib.soj_nvl(e.soj, 'n')
              ELSE NULL
            END AS STRING
          ) AS cguid,
          user_id,
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
          CAST(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_url_decode_escapes(sojlib.soj_nvl(soj, 'deeplink'), '%'),
              '%'
            ) AS STRING
          ) AS deeplink,
          CAST(
            CASE
              WHEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') IS NOT NULL
              AND sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP') NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_parse_clientinfo(e.client_data, 'RemoteIP')
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                1
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                1
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                1
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                1
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                1
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                1
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                2
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ';',
                2
              )
              WHEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              ) IS NOT NULL
              AND sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              ) NOT LIKE ALL (
                '10.%',
                '172.16.%',
                '192.168.%',
                '127.0.0.1',
                '%,10.%',
                '%,172.16.%',
                '%,192.168.%',
                '%,127.0.0.1%'
              ) THEN sojlib.soj_list_get_val_by_idx(
                sojlib.soj_parse_clientinfo(e.client_data, 'ForwardedFor'),
                ',',
                3
              )
              ELSE NULL
            END AS STRING
          ) AS external_ip,
          CAST(
            CASE
              WHEN sojlib.soj_nvl(e.soj, 'ref') IS NOT NULL
              AND sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%'),
                  '%'
                ),
                '%'
              ) NOT LIKE ALL ('%//rover.%', 'null') THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'ref'), '%')
              WHEN sojlib.soj_nvl(e.soj, 'rurl') IS NOT NULL
              AND sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'rurl'), '%'),
                  '%'
                ),
                '%'
              ) NOT LIKE ALL ('%//rover.%', 'null') THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(e.soj, 'rurl'), '%')
              WHEN sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                'dlsource'
              ) IS NOT NULL
              AND sojlib.soj_url_decode_escapes(
                sojlib.soj_nvl(
                  sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                  'dlsource'
                ),
                '%'
              ) NOT LIKE ALL(
                'google',
                'ntray',
                'ninbox',
                'unknown',
                'null',
                '%//rover.%'
              ) THEN sojlib.soj_url_decode_escapes(
                sojlib.soj_nvl(
                  sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                  'dlsource'
                ),
                '%'
              )
              ELSE NULL
            END AS STRING
          ) AS referrer,
          CAST(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_get_url_params(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(sojlib.soj_nvl(soj, 'deeplink'), '%'),
                    '%'
                  )
                ),
                '_trkparms'
              ),
              '%'
            ) AS STRING
          ) AS trkparms,
          CAST(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(sojlib.soj_nvl(url_query_string, 'lv'), '%'),
                'dlsource'
              ),
              '%'
            ) AS string
          ) AS dlsource
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
          AND e.page_id IN (2367320)
          /* Deep Link Action */
      ) d
  ) d1;

UPDATE p_soj_cl_t.temp_pre2_12aa_v1
SET
    dlk_itm = CAST(
                    CASE
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'itm'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'itm')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'id'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'id')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'itemid'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'itemid')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'item'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'item')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/itm/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/i/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/itm/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/i/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itm'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itm')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'id'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'id')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itemid'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itemid')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'item'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'item')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'iid'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'iid')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itemId'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'itemId')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(trkparms, 'itm'), 18, 0) = 1
                            THEN sojlib.soj_nvl(trkparms, 'itm')
                      WHEN sojlib.soj_get_url_path(deeplink) LIKE '/itm/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/')
                      WHEN sojlib.soj_get_url_path(deeplink) LIKE '/i/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/')
                      ELSE NULL
                    END AS DECIMAL(18, 0)
                  ),
  dlk_epid = CAST(
                    CASE
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'epid'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'epid')
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/p/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/')
                      WHEN sojlib.is_decimal(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'epid'), 18, 0) = 1
                            THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'epid')
                      WHEN sojlib.soj_get_url_path(deeplink) LIKE '/p/%'
                            AND sojlib.is_decimal(sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/'), 18, 0) = 1
                            THEN sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/')
                      ELSE NULL
                    END AS DECIMAL(18, 0)
                  ),
  dlk_sqr = CAST(sojlib.soj_collapse_whitespace(sojlib.soj_replace_char(
                                                                            CASE
                                                                              WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                                                                                    AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'nkw') IS NOT NULL
                                                                                    THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'nkw'), '%')
                                                                              WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                                                                                    AND sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'kw') IS NOT NULL
                                                                                    THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(referrer), 'kw'), '%')
                                                                              WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'nkw') IS NOT NULL
                                                                                    THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'nkw'), '%')
                                                                              WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'kw') IS NOT NULL
                                                                                    THEN sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'kw'), '%')
                                                                              ELSE NULL
                                                                            END,
                                                                            '+',
                                                                            ' '
                                                                       )
                                               ) AS STRING
                  ),
  dlk_bnid = CAST(
                    CASE
                      WHEN sojlib.soj_get_url_domain(referrer) LIKE '%.ebay.%'
                            AND sojlib.soj_get_url_path(referrer) LIKE '/b/%'
                            AND sojlib.is_decimal(replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 'bn_', ''), 18, 0) = 1
                            THEN replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path(referrer), '/'), 'bn_', '')
                      WHEN sojlib.soj_get_url_path(deeplink) LIKE '/b/%'
                            AND sojlib.is_decimal(replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/'), 'bn_', ''), 18, 0) = 1
                            THEN replace(sojlib.soj_list_last_element(sojlib.soj_get_url_path(deeplink), '/'), 'bn_', '')
                      ELSE NULL
                    END AS DECIMAL(18, 0)
                 );

UPDATE p_soj_cl_t.temp_pre2_12aa_v1 dl
SET
    sabg = cast(
                CASE
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabg') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabg')
                  WHEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabg') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabg')
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabg') IS NOT NULL THEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabg')
                  ELSE NULL
                END AS STRING
              ),
  dlguid = CAST(
                CASE
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabg') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabg')
                  WHEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabg') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabg')
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabg') IS NOT NULL THEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabg')
                  ELSE NULL
                END AS STRING
              ),
  sabc = CAST(
                CASE
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabc') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'sabc')
                  WHEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabc') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mpre'), '%'), 'sabc')
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabc') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'sabc')
                  ELSE NULL
                END AS STRING
              ),
  gclid = CAST(
                CASE
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'gclid') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'gclid')
                  WHEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'referrer'), '%'), '%'), 'gclid') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'referrer'),'%'), '%'), 'gclid')
                  WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ul_ref'), '%'), '%')), 'gclid') IS NOT NULL
                        THEN sojlib.soj_nvl(sojlib.soj_get_url_params(sojlib.soj_url_decode_escapes(sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ul_ref'), '%'), '%')), 'gclid')
                  WHEN sojlib.soj_nvl(
                    sojlib.soj_get_url_params(
                      sojlib.soj_url_decode_escapes(
                        sojlib.soj_nvl(
                          sojlib.soj_get_url_params(
                            sojlib.soj_url_decode_escapes(
                              sojlib.soj_url_decode_escapes(
                                sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'referrer'),
                                '%'
                              ),
                              '%'
                            )
                          ),
                          'ul_ref'
                        ),
                        '%'
                      )
                    ),
                    'gclid'
                  ) IS NOT NULL THEN sojlib.soj_nvl(
                    sojlib.soj_get_url_params(
                      sojlib.soj_url_decode_escapes(
                        sojlib.soj_nvl(
                          sojlib.soj_get_url_params(
                            sojlib.soj_url_decode_escapes(
                              sojlib.soj_url_decode_escapes(
                                sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'referrer'),
                                '%'
                              ),
                              '%'
                            )
                          ),
                          'ul_ref'
                        ),
                        '%'
                      )
                    ),
                    'gclid'
                  )
                  WHEN sojlib.soj_nvl(
                    sojlib.soj_get_url_params(
                      sojlib.soj_url_decode_escapes(
                        sojlib.soj_url_decode_escapes(
                          sojlib.soj_nvl(
                            sojlib.soj_get_url_params(
                              sojlib.soj_url_decode_escapes(
                                sojlib.soj_url_decode_escapes(
                                  sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ul_ref'),
                                  '%'
                                ),
                                '%'
                              )
                            ),
                            'ul_ref'
                          ),
                          '%'
                        ),
                        '%'
                      )
                    ),
                    'gclid'
                  ) IS NOT NULL THEN sojlib.soj_nvl(
                    sojlib.soj_get_url_params(
                      sojlib.soj_url_decode_escapes(
                        sojlib.soj_url_decode_escapes(
                          sojlib.soj_nvl(
                            sojlib.soj_get_url_params(
                              sojlib.soj_url_decode_escapes(
                                sojlib.soj_url_decode_escapes(
                                  sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ul_ref'),
                                  '%'
                                ),
                                '%'
                              )
                            ),
                            'ul_ref'
                          ),
                          '%'
                        ),
                        '%'
                      )
                    ),
                    'gclid'
                  )
                  ELSE NULL
                END AS STRING
              ),
  dl_type = CASE
                WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'isnotif') = '1'
                    THEN 'nactn'
                WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'is_from_notification') = '1'
                    THEN 'nactn'
                WHEN sojlib.soj_extract_nvp(trkparms, 'ni_nt', '|', ':') IS NOT NULL
                    THEN 'nhubactn'
                WHEN rotation_string IN ('711-58542-18990-19', '711-58542-18990-20')
                    THEN 'sab'
                WHEN rover_url like '%/roverns/%'
                    THEN 'roverns'
                WHEN rover_url IS NOT NULL
                    THEN 'rover'
                WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mkevt') = '1'
                    THEN 'mkevt'
                WHEN dlsource = 'ntray'
                    THEN 'nactn'
                ELSE 'other'
          END,
  rvrid = CASE
            WHEN sojlib.is_decimal(
              COALESCE(
                sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'rvr_id'),
                sojlib.soj_nvl(sojlib.soj_url_decode_escapes(sojlib.soj_get_url_params(rover_url), '%'), 'rvr_id')
              ),
              18,
              0
            ) = 1 THEN COALESCE(
              sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'rvr_id'),
              sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(sojlib.soj_get_url_params(rover_url), '%'),
                'rvr_id'
              )
            )
            ELSE NULL
          END,
  tracking_partner = CASE
                        WHEN sojlib.is_integer(
                          CASE
                            WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 3) = '' THEN NULL
                            ELSE sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 3)
                          END
                        ) = 1 THEN CAST(
                          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 3) AS INTEGER
                        )
                        ELSE NULL
                      END,
                      channel = CASE
                        WHEN sojlib.is_integer(
                          CASE
                            WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 5) = '' THEN NULL
                            ELSE sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 5)
                          END
                        ) = 1 THEN CAST(
                          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 5) AS INTEGER
                        )
                        WHEN sojlib.is_integer(
                          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mkcid')
                        ) = 1 then CAST(
                          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mkcid') AS INTEGER
                        )
                        ELSE NULL
                      END,
  campaign_id = CAST(
                    COALESCE(
                      sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'campid'),
                      sojlib.soj_nvl(
                        sojlib.soj_get_url_params(Lower(rover_url)),
                        'campid'
                      ),
                      sojlib.soj_nvl(
                        sojlib.soj_get_url_params(rover_url),
                        'amp;campid'
                      )
                    ) AS STRING
              ),
  crlp = CAST(
                COALESCE(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'crlp'),
                  sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'crlp')
                ) AS STRING
            ),
  geo_id = CAST(
                COALESCE(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'geo_id'),
                  sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'geo_id')
                ) AS STRING
              ),
  bought_keyword = replace(
    CASE
      WHEN dl.rover_url LIKE '%keyword=%'
            THEN sojlib.soj_collapse_whitespace(
                    sojlib.soj_replace_char(
                         sojlib.soj_url_decode_escapes(sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'keyword'), '%'),
                         '+',
                         ' '
                    )
                )
      WHEN dl.deeplink like '%keyword=%'
            THEN sojlib.soj_collapse_whitespace(
                    sojlib.soj_replace_char(
                      sojlib.soj_url_decode_escapes(
                        sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'keyword'),
                        '%'
                      ),
                      '+',
                      ' '
                    )
                  )
      WHEN dl.rover_url LIKE '%rawquery=%'
          THEN sojlib.soj_collapse_whitespace(
                sojlib.soj_replace_char(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'rawquery'),
                    '%'
                  ),
                  '+',
                  ' '
                )
              )
      WHEN dl.deeplink LIKE '%rawquery=%'
          THEN sojlib.soj_collapse_whitespace(
                sojlib.soj_replace_char(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'rawquery'),
                    '%'
                  ),
                  '+',
                  ' '
                )
              )
      ELSE NULL
    END,
    '&',
    '%26'
  );

UPDATE
  rc
FROM
  p_soj_cl_t.temp_pre2_12aa_v1 rc,
  (
    SELECT
      rotation_string,
      MIN(rotation_id) AS rotation_id,
      MIN(campaign_id) AS campaign_id,
      MIN(mpx_chnl_id) AS mpx_chnl_id
    FROM
      choco_data_v.dw_mpx_rotations
      GROUP BY 1
  ) r
SET rc.mpx_chnl_id = CASE
                        WHEN r.campaign_id = 137245 THEN 97
                        ELSE r.mpx_chnl_id
                      END,
  rc.rotation_id = r.rotation_id
WHERE
  rc.rotation_string = r.rotation_string
  AND rc.rotation_string IS NOT NULL;

UPDATE
  p_soj_cl_t.temp_pre2_12aa_v1 dl
SET src_string = CAST(
    't=' || date_format(
      CAST(CAST(dl.event_timestamp as timestamp) as STRING),
      'yyyy-MM-dd HH:mm:ss.SSS'
    ) || '&src=dl' || '&sn=' || CAST(dl.seqnum AS STRING) || CASE
      WHEN rvrid IS NULL THEN ''
      WHEN rvrid = 0 THEN ''
      ELSE '&rvr=' || CAST(CAST(rvrid AS BIGINT) AS STRING)
    END || CASE
      WHEN rotation_id IS NULL THEN ''
      ELSE '&rot=' || CAST(CAST(dl.rotation_id AS BIGINT) AS STRING)
    END || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ext'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ext')
        ) IS NOT NULL THEN '&ext=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ext'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ext')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.is_bigint(
          Trim(
            replace(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'emid'),
                '%'
              ),
              '=',
              ''
            )
          )
        ) = 1
        AND sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'emid') IS NOT NULL THEN '&emid=' || Trim(
          replace(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'emid'),
              '%'
            ),
            '=',
            ''
          )
        )
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emid') IS NOT NULL THEN '&emid=' || sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emid')
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN char_length(
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'crd')
        ) = 14 THEN '&crd=' || sojlib.soj_nvl(sojlib.soj_get_url_params(rover_URL), 'crd')
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'crd') IS NOT NULL THEN '&crd=' || sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'crd')
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'email'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'email')
        ) IS NOT NULL THEN '&email=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'email'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'email')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'segname') IS NOT NULL THEN '&segname=' || sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'segname')
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'segname') IS NOT NULL THEN '&segname=' || sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'segname')
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'bannercid'
        ) IS NOT NULL THEN '&bannercid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'bannercid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'bannercid'
        ) IS NOT NULL THEN '&bannercid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'bannercid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'bannerrid'
        ) IS NOT NULL THEN '&bannerrid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'bannerrid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'bannerrid'
        ) IS NOT NULL THEN '&bannerrid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'bannerrid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'trkid'
        ) IS NOT NULL THEN '&trkid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'trkid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'trkid'
        ) IS NOT NULL THEN '&trkid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'trkid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'utpid'
        ) IS NOT NULL THEN '&utpid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'utpid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'utpid'
        ) IS NOT NULL THEN '&utpid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'utpid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'mesgid'
        ) IS NOT NULL THEN '&mesgid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'mesgid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'mesgid'
        ) IS NOT NULL THEN '&mesgid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'mesgid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'plmtid'
        ) IS NOT NULL THEN '&plmtid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'plmtid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'plmtid'
        ) IS NOT NULL THEN '&plmtid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'plmtid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'recoid'
        ) IS NOT NULL THEN '&recoid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'recoid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'recoid'
        ) IS NOT NULL THEN '&recoid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'recoid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'recopos'
        ) IS NOT NULL THEN '&recopos=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'recopos'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'recopos'
        ) IS NOT NULL THEN '&recopos=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'recopos'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'cnvid'
        ) IS NOT NULL THEN '&cnvid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(lower(rover_url)),
          'cnvid'
        )
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'cnvid'
        ) IS NOT NULL THEN '&cnvid=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'cnvid'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'mktype'
        ) IS NOT NULL THEN '&mktype=' || sojlib.soj_nvl(
          lower(sojlib.soj_get_url_params(deeplink)),
          'mktype'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymcb'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymcb')
        ) IS NOT NULL THEN '&ymcb=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymcb'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymcb')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymdivid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymdivid')
        ) IS NOT NULL THEN '&ymdivid=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymdivid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymdivid')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymhid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymhid')
        ) IS NOT NULL THEN '&ymhid=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymhid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymhid')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'yminstc'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'yminstc')
        ) IS NOT NULL THEN '&yminstc=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'yminstc'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'yminstc')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymmmid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymmmid')
        ) IS NOT NULL THEN '&ymmmid=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymmmid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymmmid')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymsid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymsid')
        ) IS NOT NULL THEN '&ymsid=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymsid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymsid')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymuid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymuid')
        ) IS NOT NULL THEN '&ymuid=' || COALESCE(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'ymuid'),
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'ymuid')
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'mppid') IS NOT NULL
        AND sojlib.is_integer(
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'mppid')
        ) = 1 THEN '&mppid=' || sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'mppid')
        WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mppid') IS NOT NULL
        AND sojlib.is_integer(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mppid')
        ) = 1 THEN '&mppid=' || sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'mppid')
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'smsid'),
          '%'
        ) IS NOT NULL THEN '&smsid=' || sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'smsid'),
          '%'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'did'),
          '%'
        ) IS NOT NULL THEN '&did=' || sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'did'),
          '%'
        )
        ELSE ''
      END AS STRING
    ) || CAST(
      CASE
        WHEN sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'media'),
          '%'
        ) IS NOT NULL THEN '&media=' || lower(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'media'),
            '%'
          )
        )
        ELSE ''
      END AS STRING
    ) AS STRING
  );

UPDATE
  p_soj_cl_t.temp_pre2_12aa_v1 dl
SET dl_type = 'email',
  src_string = COALESCE(src_string, '') || CAST(
    CAST(
      CASE
        WHEN char_length(
          sojlib.soj_nvl(
            sojlib.soj_get_url_params(COALESCE(rover_url)),
            'euid'
          )
        ) = 32 THEN '&euid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(COALESCE(rover_url)),
          'euid'
        )
        WHEN char_length(
          sojlib.soj_nvl(
            sojlib.soj_get_url_params(COALESCE(deeplink)),
            'euid'
          )
        ) = 32 THEN '&euid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(COALESCE(deeplink)),
          'euid'
        )
        WHEN char_length(
          sojlib.soj_nvl(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
              '%'
            ),
            'euid'
          )
        ) = 32 THEN '&euid=' || sojlib.soj_nvl(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
            '%'
          ),
          'euid'
        )
        WHEN char_length(
          sojlib.soj_nvl(
            sojlib.soj_get_url_params(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_nvl(
                  sojlib.soj_get_url_params(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                      '%'
                    )
                  ),
                  'loc'
                ),
                '%'
              )
            ),
            'euid'
          )
        ) = 32 THEN '&euid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_get_url_params(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  )
                ),
                'loc'
              ),
              '%'
            )
          ),
          'euid'
        )
        WHEN char_length(
          sojlib.soj_nvl(
            sojlib.soj_get_url_params(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_nvl(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_url_decode_escapes(
                      sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                      '%'
                    ),
                    '%'
                  ),
                  'loc'
                ),
                '%'
              )
            ),
            'euid'
          )
        ) = 32 THEN '&euid=' || sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  ),
                  '%'
                ),
                'loc'
              ),
              '%'
            )
          ),
          'euid'
        )
        ELSE ''
      END AS STRING
    ) || CASE
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(COALESCE(deeplink, rover_url)),
          'emid'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(COALESCE(deeplink, rover_url)),
        'emid'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
            '%'
          ),
          'emid'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
          '%'
        ),
        'emid'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_get_url_params(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  )
                ),
                'loc'
              ),
              '%'
            )
          ),
          'emid'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(
              sojlib.soj_get_url_params(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                  '%'
                )
              ),
              'loc'
            ),
            '%'
          )
        ),
        'emid'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  ),
                  '%'
                ),
                'loc'
              ),
              '%'
            )
          ),
          'emid'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                  '%'
                ),
                '%'
              ),
              'loc'
            ),
            '%'
          )
        ),
        'emid'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(COALESCE(deeplink, rover_url)),
          'bu'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(COALESCE(deeplink, rover_url)),
        'bu'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
            '%'
          ),
          'bu'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_url_decode_escapes(
          sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
          '%'
        ),
        'bu'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_get_url_params(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  )
                ),
                'loc'
              ),
              '%'
            )
          ),
          'bu'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(
              sojlib.soj_get_url_params(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                  '%'
                )
              ),
              'loc'
            ),
            '%'
          )
        ),
        'bu'
      )
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_url_decode_escapes(
                    sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                    '%'
                  ),
                  '%'
                ),
                'loc'
              ),
              '%'
            )
          ),
          'bu'
        ),
        18,
        0
      ) = 1 THEN '&emid=' || sojlib.soj_nvl(
        sojlib.soj_get_url_params(
          sojlib.soj_url_decode_escapes(
            sojlib.soj_nvl(
              sojlib.soj_url_decode_escapes(
                sojlib.soj_url_decode_escapes(
                  sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'loc'),
                  '%'
                ),
                '%'
              ),
              'loc'
            ),
            '%'
          )
        ),
        'bu'
      )
      ELSE ''
    END || CASE
      WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid') LIKE 'e%.%' THEN '&e=' || CAST(
        SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
            '\\.',
            1
          ),
          2,
          9999
        ) AS STRING
      )
      WHEN sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid') LIKE 'e%' THEN '&e=' || CAST(
        SUBSTR(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
          2,
          9999
        ) AS STRING
      )
      WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4) LIKE 'e%.%' THEN '&e=' || CAST(
        SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
            '\\.',
            1
          ),
          2,
          9999
        ) AS STRING
      )
      WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4) LIKE 'e%' THEN '&e=' || CAST(
        SUBSTR(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
          2,
          9999
        ) AS STRING
      )
      ELSE ''
    END || CASE
      WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4) LIKE '%.m%.%' OR sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid') LIKE '%.m%.%' THEN CASE
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
          '\\.',
          2
        ) LIKE 'm%' THEN '&m=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
            '\\.',
            2
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
          '\\.',
          3
        ) LIKE 'm%' THEN '&m=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
            '\\.',
            3
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
          '\\.',
          2
        ) LIKE 'm%' THEN '&m=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
            '\\.',
            2
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
          '\\.',
          3
        ) LIKE 'm%' THEN '&m=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
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
      WHEN sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4) LIKE '%.l%' OR sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid') LIKE '%.l%' THEN CASE
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
          '\\.',
          3
        ) LIKE 'l%' THEN '&l=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
            '\\.',
            3
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
          '\\.',
          4
        ) LIKE 'l%' THEN '&l=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_nvl(sojlib.soj_get_url_params(deeplink), 'emsid'),
            '\\.',
            4
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
          '\\.',
          3
        ) LIKE 'l%' THEN '&l=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
            '\\.',
            3
          ),
          2,
          7
        )
        WHEN sojlib.soj_list_get_val_by_idx(
          sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
          '\\.',
          4
        ) LIKE 'l%' THEN '&l=' || SUBSTR(
          sojlib.soj_list_get_val_by_idx(
            sojlib.soj_list_get_val_by_idx(sojlib.soj_get_url_path(rover_url), '/', 4),
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
      WHEN sojlib.is_decimal(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'mpre'),
              '%'
            )
          ),
          'co'
        ),
        18,
        0
      ) = 1 THEN '&co=' || CAST(
        sojlib.soj_nvl(
          sojlib.soj_get_url_params(
            sojlib.soj_url_decode_escapes(
              sojlib.soj_nvl(sojlib.soj_get_url_params(rover_url), 'mpre'),
              '%'
            )
          ),
          'co'
        ) AS STRING
      )
      ELSE ''
    END AS STRING
  )
WHERE
  channel IN (7, 8);

UPDATE
  t12a
FROM
  p_soj_cl_t.temp_pre2_12aa_v1 t12a,
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
    t12a.src_string = case when t12a.src_string is null
                                then ''
                            else t12a.src_string
                       end
                        || '&epn_campid=' || CAST(t12a.campaign_id AS STRING)
                        || '&epn_group=' || CAST(case when r.partner_group is not null then r.partner_group else 'unmapped' end AS STRING)
WHERE
  r.campaign_id = t12a.campaign_id
  and t12a.mpx_chnl_id = 6
  and t12a.campaign_id is not null;


DELETE FROM p_soj_cl_t.deeplink_dtl_w1 WHERE session_start_dt BETWEEN '2024-08-12' AND '2024-08-13';

INSERT INTO p_soj_cl_t.deeplink_dtl_w1
(
    guid,
    session_skey,
    site_id,
    seqnum,
    event_timestamp,
    cguid,
    user_id,
    app_id,
    deeplink,
    external_ip,
    referrer,
    trkparms,
    dlsource,
    rover_url,
    rotation_string,
    dlk_itm,
    dlk_epid,
    dlk_sqr,
    dlk_bnid,
    sabg,
    sabc,
    sabs,
    dlguid,
    gclid,
    dl_type,
    rvrid,
    tracking_partner,
    channel,
    campaign_id,
    crlp,
    geo_id,
    bought_keyword,
    mpx_chnl_id,
    rotation_id,
    src_string,
    link_src,
    session_start_dt,
    updated_ts
  )
select
  guid,
  session_skey,
  site_id,
  seqnum,
  event_timestamp,
  cguid,
  user_id,
  app_id,
  deeplink,
  external_ip,
  referrer,
  trkparms,
  dlsource,
  rover_url,
  rotation_string,
  dlk_itm,
  dlk_epid,
  dlk_sqr,
  dlk_bnid,
  sabg,
  sabc,
  sabs,
  dlguid,
  gclid,
  dl_type,
  rvrid,
  tracking_partner,
  channel,
  campaign_id,
  crlp,
  geo_id,
  bought_keyword,
  mpx_chnl_id,
  rotation_id,
  CASE
    WHEN src_string IS NULL THEN "dlk_event_key=guid:" || guid || "|sskey:" || CAST(session_skey AS STRING) || "|seq:" || CAST(seqnum AS STRING) || "|ts:" || date_format(
      CAST(CAST(event_timestamp AS TIMESTAMP) AS STRING),
      'yyyy-MM-dd HH:mm:ss.SSS'
    )
    ELSE src_string || "&dlk_event_key=guid:" || guid || "|sskey:" || CAST(session_skey AS STRING) || "|seq:" || CAST(seqnum AS STRING) || "|ts:" || date_format(
      CAST(CAST(event_timestamp AS TIMESTAMP) AS STRING),
      'yyyy-MM-dd HH:mm:ss.SSS'
    )
  END AS src_string,
  link_src,
  session_start_dt,
  CURRENT_TIMESTAMP
FROM
  p_soj_cl_t.temp_pre2_12aa_v1;