DROP TABLE IF EXISTS p_soj_cl_t.temp_csess11a_v9;
CREATE TABLE p_soj_cl_t.temp_csess11a_v9 USING PARQUET AS
SELECT
	*
FROM
	p_soj_cl_t.mcs_event_dtl
WHERE
	session_start_dt BETWEEN '2024-05-31' AND '2024-06-01';