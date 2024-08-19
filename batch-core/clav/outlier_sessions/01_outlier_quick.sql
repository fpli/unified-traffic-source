DELETE FROM p_soj_cl_t.outlier_sessions WHERE soj_data_dt = '2024-06-01';

INSERT INTO p_soj_cl_t.outlier_sessions
    (guid, session_skey, session_start_dt, soj_data_dt, bot_flag, event_cnt)
SELECT
    e.guid, e.session_skey, e.session_start_dt, CAST(e.event_timestamp AS DATE) AS soj_data_dt, NULL, count(*) as event_cnt
FROM
    ubi_v.ubi_event e
WHERE
    e.session_start_dt BETWEEN '2024-05-29' AND '2024-05-31' AND CAST(e.event_timestamp AS DATE) >= '2024-06-01'
GROUP BY 1,2,3,4,5;