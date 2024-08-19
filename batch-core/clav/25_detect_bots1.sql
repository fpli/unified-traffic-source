UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			s1.guid,
			s1.session_skey,
			s1.session_start_dt
		FROM
			ubi_v.ubi_session s1
		WHERE
			s1.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01'
			AND s1.bot_flag > 0
			AND s1.bot_flag NOT IN (12,15)
		GROUP BY 1,2,3
	) s
SET
	t2.bot_session = 1,
	t2.bot_flags64 = t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1 AS BIGINT) > 0 THEN 0 ELSE 1 END
WHERE
	t2.guid = s.guid AND t2.session_skey = s.session_skey AND t2.session_start_dt = s.session_start_dt;

UPDATE
	p_soj_cl_t.temp_csess2a_v9 t2
SET
	bot_session = CASE
						WHEN bot_flags64 + CASE WHEN t2.lndg_page_id = 2047675
							 						AND t2.lndg_page_url LIKE ALL ('%ssn=%','%seller_type=%')
							 						AND COALESCE(signedin_user_id, mapped_user_id) IS NULL
							 						AND ((valid_page_count <= 2 AND lndg_page_id = exit_page_id) OR (vi_cnt + 1 >= valid_page_count AND lndg_page_id = exit_page_id))
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8796093022208 AS BIGINT) > 0
																THEN 0
															ELSE 8796093022208
														END
												ELSE 0
											END
										+ CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
													THEN CASE
															 WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4 AS BIGINT) > 0
																THEN 0
															 ELSE 4
														 END
											   WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
													THEN CASE
															 WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4 AS BIGINT) > 0
																THEN 0
															 ELSE 4
														 END
												ELSE 0
											END
										+ CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8 AS BIGINT) > 0
																THEN 0
															ELSE 8
														END
												WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8 AS BIGINT) > 0
																THEN 0
															ELSE 8
														END
												ELSE 0
											END
										+ CASE WHEN t2.valid_page_count > 5 AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND t2.siid_cnt <= 1 AND (CAST(t2.session_flags64 AS BIGINT) & CAST(8 AS BIGINT) = 8)
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4096 AS BIGINT) > 0
																THEN 0
															ELSE 4096
														END
												ELSE 0
										  	END
										+ CASE WHEN t2.agent_id IS NULL AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND siid_cnt = 0
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8192 AS BIGINT) > 0
																THEN 0
															ELSE 8192
														END
												ELSE 0
											END
										+ CASE WHEN t2.valid_page_count > 3000
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(16384 AS BIGINT) > 0
																THEN 0
															ELSE 16384
														END
												ELSE 0
											END
										+ CASE WHEN t2.valid_page_count > 400 AND vi_cnt > (0.97 * valid_page_count) AND vi_cnt > (gr_cnt * 50)
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2097152 AS BIGINT) > 0
																THEN 0
															ELSE 2097152
														END
												ELSE 0
											END
										+ CASE WHEN t2.lndg_page_id = 6118 AND (lndg_page_url LIKE 'https://signin.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%' OR lndg_page_url LIKE 'https://www.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%')
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(268435456 AS BIGINT) > 0
																THEN 0
															ELSE 268435456
														END
												ELSE 0
											END
										+ CASE WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2481888 AND lndg_page_id = exit_page_id AND valid_page_count <= 5
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
																THEN 0
															ELSE 2147483648
														END
												WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND lndg_page_id in (2481888,4375194,4445145) AND lndg_page_id = exit_page_id AND valid_page_count <= 2 and sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
																THEN 0
															ELSE 2147483648
														END
												WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2380424 AND sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
																THEN 0
															ELSE 2147483648
														END
												ELSE 0
											END
										+ CASE WHEN (lndg_page_id = 2059331 AND sojlib.soj_nvl(session_traffic_source_details, 'rd') LIKE 'fyp.ebay.%') OR (lndg_page_id IN (2487282,2487283) AND referrer LIKE 'http%://www.ebay.%/signin/s') OR (lndg_page_id IN (2561325, 2561567) AND exit_page_id IN (2561325, 2561567) AND max_sc_seqnum <= 3 AND lndg_page_url NOT LIKE '%mkevt%')
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4294967296 AS BIGINT) > 0
																THEN 0
															ELSE 4294967296
														END
												ELSE 0
											END
										+ CASE WHEN primary_app_id = 2571 AND regexp_extract(guid, '[^a-z0-9]', 0) = '' AND sojlib.soj_guid_ts(guid) > start_timestamp + INTERVAL '8' HOUR AND (COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) NOT LIKE ALL ('2.%', '3.%') OR COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) IS NULL)
													THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(68719476736 AS BIGINT) > 0
																THEN 0
															ELSE 68719476736
														END
												ELSE 0
											END
										+ CASE WHEN t2.agent_id = -7663467763370095509 AND t2.lndg_page_id IN (2487282, 2487283, 2481888) AND t2.exit_page_id IN (2487282, 2487283) AND t2.valid_page_count <= 3
													THEN 1099511627776
											   WHEN t2.lndg_page_url IN ('https://www.ebay.co.uk/itm/264713295528', 'https://www.ebay.co.uk/itm/402245598884')
											   		THEN
														CASE
															WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1099511627776 AS BIGINT) > 0
																THEN 0
															ELSE 1099511627776
														END
												ELSE 0
											END > 0
							THEN 1
						ELSE 0
				END,
bot_flags64 = bot_flags64 + CASE WHEN t2.lndg_page_id = 2047675 AND t2.lndg_page_url LIKE ALL ('%ssn=%','%seller_type=%') AND COALESCE(signedin_user_id, mapped_user_id) IS NULL AND ((valid_page_count <= 2 AND lndg_page_id = exit_page_id) OR (vi_cnt + 1 >= valid_page_count AND lndg_page_id = exit_page_id))
									  THEN
									  	CASE
											WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8796093022208 AS BIGINT) > 0
												THEN 0
											ELSE 8796093022208
										END
								 ELSE 0
							END
						 + CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
						 				THEN
									 		CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4 AS BIGINT) > 0
													THEN 0
												ELSE 4
											END
								WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4 AS BIGINT) > 0
													THEN 0
												ELSE 4
											END
									ELSE 0
							END
						+ CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8 AS BIGINT) > 0
													THEN 0
												ELSE 8
											END
							  	WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8 AS BIGINT) > 0
													THEN 0
												ELSE 8
											END
								ELSE 0
							END
						+ CASE WHEN t2.valid_page_count > 5 AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND t2.siid_cnt <= 1 AND (CAST(t2.session_flags64 AS BIGINT) & CAST(8 AS BIGINT) = 8)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4096 AS BIGINT) > 0
													THEN 0
												ELSE 4096
											END
								ELSE 0
							END
						+ CASE WHEN t2.agent_id IS NULL AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND siid_cnt = 0
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8192 AS BIGINT) > 0
													THEN 0
												ELSE 8192
											END
								ELSE 0
							END
						+ CASE WHEN t2.valid_page_count > 3000
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(16384 AS BIGINT) > 0
													THEN 0
												ELSE 16384
											END
								ELSE 0
							END
						+ CASE WHEN t2.valid_page_count > 400 AND vi_cnt > (0.97 * valid_page_count) AND vi_cnt > (gr_cnt * 50)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2097152 AS BIGINT) > 0
													THEN 0
												ELSE 2097152
											END
								ELSE 0
							END
						+ CASE WHEN t2.lndg_page_id = 6118 AND (lndg_page_url LIKE 'https://signin.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%' OR lndg_page_url LIKE 'https://www.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%')
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(268435456 AS BIGINT) > 0
													THEN 0
												ELSE 268435456
											END
								ELSE 0
							END
						+ CASE WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2481888 AND lndg_page_id = exit_page_id AND valid_page_count <= 5
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
													THEN 0
												ELSE 2147483648
											END
							    WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND lndg_page_id in (2481888,4375194,4445145) AND lndg_page_id = exit_page_id AND valid_page_count <= 2 and sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
													THEN 0
												ELSE 2147483648
											END
								WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2380424 AND sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
													THEN 0
												ELSE 2147483648
											END
								ELSE 0
							END
						+ CASE WHEN (lndg_page_id = 2059331 AND sojlib.soj_nvl(session_traffic_source_details, 'rd') LIKE 'fyp.ebay.%') OR (lndg_page_id IN (2487282,2487283) AND referrer LIKE 'http%://www.ebay.%/signin/s') OR (lndg_page_id IN (2561325, 2561567) AND exit_page_id IN (2561325, 2561567) AND max_sc_seqnum <= 3 AND lndg_page_url NOT LIKE '%mkevt%')
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(4294967296 AS BIGINT) > 0
													THEN 0
												ELSE 4294967296
											END
								ELSE 0
							END
						+ CASE WHEN primary_app_id = 2571 AND regexp_extract(guid, '[^a-z0-9]', 0) = '' AND sojlib.soj_guid_ts(guid) > start_timestamp + INTERVAL '8' HOUR AND (COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) NOT LIKE ALL ('2.%', '3.%') OR COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) IS NULL)
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(68719476736 AS BIGINT) > 0
													THEN 0
												ELSE 68719476736
											END
								ELSE 0
							END
						+ CASE WHEN t2.agent_id = -7663467763370095509 AND t2.lndg_page_id IN (2487282, 2487283, 2481888) AND t2.exit_page_id IN (2487282, 2487283) AND t2.valid_page_count <= 3
										THEN 1099511627776
								WHEN t2.lndg_page_url IN ('https://www.ebay.co.uk/itm/264713295528', 'https://www.ebay.co.uk/itm/402245598884')
										THEN
											CASE
												WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(1099511627776 AS BIGINT) > 0
													THEN 0
												ELSE 1099511627776
											END
								ELSE 0
						  END
WHERE
	  CASE WHEN t2.lndg_page_id = 2047675 AND t2.lndg_page_url LIKE ALL ('%ssn=%','%seller_type=%') AND COALESCE(signedin_user_id, mapped_user_id) IS NULL AND ((valid_page_count <= 2 AND lndg_page_id = exit_page_id) OR (vi_cnt + 1 >= valid_page_count AND lndg_page_id = exit_page_id))
			THEN 8796093022208
		 ELSE 0
	  END
	+ CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
				THEN 4
		   WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.vi_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
		   		THEN 4
		   ELSE 0
	  END
	+ CASE WHEN t2.valid_page_count > 20 AND t2.site_id NOT IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
				THEN 8
		   WHEN t2.valid_page_count > 100 AND t2.site_id IN (100) AND t2.valid_page_count = t2.gr_cnt AND t2.siid_cnt = 0 AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878)
		   		THEN 8
		   ELSE 0
	   END
	+ CASE WHEN t2.valid_page_count > 5 AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND t2.siid_cnt <= 1 AND (CAST(t2.session_flags64 AS BIGINT) & CAST(8 AS BIGINT) = 8)
	 			THEN 4096
			ELSE 0
		END
	+ CASE WHEN t2.agent_id IS NULL AND t2.cobrand NOT IN (5,9,10) AND /**filtering out Native**/ COALESCE(t2.primary_app_id,1) NOT IN (1462, 2571, 2878) AND siid_cnt = 0
	 			THEN 8192
			ELSE 0
		END
	+ CASE WHEN t2.valid_page_count > 3000
	 			THEN 16384
			ELSE 0
		END
	+ CASE WHEN t2.valid_page_count > 400 AND vi_cnt > (0.97 * valid_page_count) AND vi_cnt > (gr_cnt * 50)
	 			THEN 2097152
			ELSE 0
		END
	+ CASE WHEN t2.lndg_page_id = 6118 AND (lndg_page_url LIKE 'https://signin.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%' OR lndg_page_url LIKE 'https://www.ebay.%/ws/eBayISAPI.dll?V4SignInAjax=&method=preinit&userid=%')
	 			THEN 268435456
			ELSE 0
		END
	+ CASE WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2481888 AND lndg_page_id = exit_page_id AND valid_page_count <= 5
	 			THEN
					CASE
						WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
								THEN 0
						ELSE 2147483648
					END
			WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND lndg_page_id in (2481888,4375194,4445145) AND lndg_page_id = exit_page_id AND valid_page_count <= 2 and sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
				THEN
					CASE
						WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
							THEN 0
						ELSE 2147483648
					END
			WHEN t2.cobrand = 6 AND primary_app_id IN (1462,2878) AND sojlib.soj_nvl(session_details, 'idfa') IS NULL AND lndg_page_id = 2380424 AND sojlib.soj_nvl(session_details, 'fg') IS NULL AND sojlib.soj_nvl(session_details, 'bg') IS NULL
				THEN
					CASE
						WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(2147483648 AS BIGINT) > 0
							THEN 0
						ELSE 2147483648
					END
			ELSE 0
		END
	+ CASE WHEN (lndg_page_id = 2059331 AND sojlib.soj_nvl(session_traffic_source_details, 'rd') LIKE 'fyp.ebay.%') OR (lndg_page_id IN (2487282,2487283) AND referrer LIKE 'http%://www.ebay.%/signin/s') OR (lndg_page_id IN (2561325, 2561567) AND exit_page_id IN (2561325, 2561567) AND max_sc_seqnum <= 3 AND lndg_page_url NOT LIKE '%mkevt%')
				THEN 4294967296
			ELSE 0
		END
	+ CASE WHEN primary_app_id = 2571 AND regexp_extract(guid, '[^a-z0-9]', 0) = '' AND sojlib.soj_guid_ts(guid) > start_timestamp + INTERVAL '8' HOUR AND (COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) NOT LIKE ALL ('2.%', '3.%') OR COALESCE(sojlib.soj_nvl(session_details, 'mav'), sojlib.soj_nvl(session_details, 'bv')) IS NULL)
				THEN 68719476736
			ELSE 0
		END
	+ CASE WHEN t2.agent_id = -7663467763370095509 AND t2.lndg_page_id IN (2487282, 2487283, 2481888) AND t2.exit_page_id IN (2487282, 2487283) AND t2.valid_page_count <= 3
				THEN 1099511627776
			WHEN t2.lndg_page_url IN ('https://www.ebay.co.uk/itm/264713295528', 'https://www.ebay.co.uk/itm/402245598884')
				THEN 1099511627776
			ELSE 0
		END
	> 0;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			ip,
			COUNT(*) AS recs
		FROM
			p_soj_cl_t.temp_csess2a_v9 t
		WHERE
			lndg_page_id IN (4853,2487283, 2487282)
			AND exit_page_id IN (4853, 2487283, 2487282)
			AND valid_page_count = 2
		GROUP BY 1
		HAVING recs >= 100
	) t
SET
	t2.bot_session = 1,
	t2.bot_flags64 =
	t2.bot_flags64 + CASE WHEN CAST(t2.bot_flags64 AS BIGINT) & CAST(8388608 AS BIGINT) > 0
								THEN 0
						ELSE 8388608
					 END
WHERE
	t2.lndg_page_id IN (4853, 2487283, 2487282)
	AND t2.exit_page_id IN (4853, 2487283, 2487282)
	AND t2.valid_page_count = 2
	AND t2.ip = t.ip;

UPDATE t2
FROM
	p_soj_cl_t.temp_csess2a_v9 t2,
	(
		SELECT
			agent_id,
			COUNT(*) AS numrecords,
			SUM(CASE WHEN cguid IS NULL THEN 1 ELSE 0 END) AS null_cguid,
			SUM(CASE WHEN s.lndg_page_id IN (2481888) AND s.exit_page_id IN (2487282,2059331) AND s.valid_page_count = 3 AND s.session_traffic_source_id = 1 THEN 1 WHEN s.lndg_page_id IN (4853, 2487283) AND s.exit_page_id IN (2487282,2059331) AND s.valid_page_count = 2 AND s.session_traffic_source_id IN (1,2) THEN 1 WHEN s.lndg_page_id IN (4853, 2487283) AND s.exit_page_id IN (2487282,2059331) AND s.valid_page_count = signin_cnt AND s.session_traffic_source_id IN (1,2) THEN 1 ELSE 0 END) AS bot_profile_recs,
			(bot_profile_recs*100.0)/numrecords AS pct_profiled,
			(null_cguid*100.0)/numrecords AS pct_nullcguid
		FROM
			p_soj_cl_t.temp_csess2a_v9 s
		WHERE
			s.lndg_page_id IN (4853,2487283,2481888)
			AND s.exit_page_id IN (2487282,2059331)
		GROUP BY 1
		HAVING
			numrecords > 100 AND (pct_nullcguid >= 98) AND (pct_profiled >= 98)
	) t
SET
	t2.bot_session = 1,
	t2.bot_flags64 = bot_flags64 + 8388608
WHERE
	t2.lndg_page_id IN (4853,2487283,2481888)
	AND t2.exit_page_id IN (2487282,2059331)
	AND CAST(t2.bot_flags64 AS BIGINT) & CAST(8388608 AS BIGINT) <> 8388608 /* not set already */
	AND t2.agent_id = t.agent_id;