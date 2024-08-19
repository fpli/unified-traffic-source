UPDATE
	p_soj_cl_t.temp_csess2a_v9
SET
	session_details = COALESCE(session_details, '')
					  	|| CASE WHEN sojlib.soj_nvl(session_details, 'dn') IS NULL AND sojlib.soj_nvl(agent_details, 'hwv') IS NOT NULL THEN '&dn=' || CAST(sojlib.soj_nvl(agent_details, 'hwv') AS STRING) ELSE '' END
						|| CASE WHEN sojlib.soj_nvl(session_details, 'bt') IS NULL AND sojlib.soj_nvl(agent_details, 'bt') IS NOT NULL THEN '&bt=' || CAST(sojlib.soj_nvl(agent_details, 'bt') AS STRING) ELSE '' END
						|| CASE WHEN sojlib.soj_nvl(session_details, 'bv') IS NULL AND sojlib.soj_nvl(agent_details, 'bv') IS NOT NULL THEN '&bv=' || CAST(sojlib.soj_nvl(agent_details, 'bv') AS STRING) ELSE '' END
						|| CASE WHEN sojlib.soj_nvl(session_details, 'ost') IS NULL AND sojlib.soj_nvl(agent_details, 'ost') IS NOT NULL THEN '&ost=' || CAST(sojlib.soj_nvl(agent_details, 'ost') AS STRING) ELSE '' END
						|| CASE WHEN sojlib.soj_nvl(session_details, 'osv') IS NULL AND sojlib.soj_nvl(agent_details, 'osv') IS NOT NULL THEN '&osv=' || CAST(sojlib.soj_nvl(agent_details, 'osv') AS STRING) ELSE '' END;

COMPACT TABLE p_soj_cl_t.temp_csess2a_v9;