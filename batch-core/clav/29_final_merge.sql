DROP TABLE IF EXISTS p_soj_cl_t.temp_csess3a_v9;
CREATE TABLE p_soj_cl_t.temp_csess3a_v9 USING delta AS
SELECT
	s.*
FROM
	p_soj_cl_t.clav_session_dtl s
WHERE
	s.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01';

DROP TABLE IF EXISTS p_soj_cl_t.temp_csess3b_v9;
CREATE TABLE p_soj_cl_t.temp_csess3b_v9 USING delta AS
SELECT
	s.*
FROM
	p_soj_cl_t.temp_csess2a_v9 s;

UPDATE t1
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
SET
	t1.cguid = t2.cguid,
	t1.valid_page_count = t2.valid_page_count,
	t1.min_sc_seqnum = t2.min_sc_seqnum,
	t1.max_sc_seqnum = t2.max_sc_seqnum,
	t1.signedin_user_id = t2.signedin_user_id,
	t1.mapped_user_id = t2.mapped_user_id,
	t1.agent_id = t2.agent_id,
	t1.social_agent_type = t2.social_agent_type,
	t1.search_agent_type = t2.search_agent_type,
	t1.device_type = t2.device_type,
	t1.agent_details = t2.agent_details,
	t1.session_cntry_id = t2.session_cntry_id,
	t1.session_rev_rollup = t2.session_rev_rollup,
	t1.ip = t2.ip,
	t1.gr_cnt = t2.gr_cnt,
	t1.gr_1_cnt = t2.gr_1_cnt,
	t1.vi_cnt = t2.vi_cnt,
	t1.homepage_cnt = t2.homepage_cnt,
	t1.myebay_cnt = t2.myebay_cnt,
	t1.signin_cnt = t2.signin_cnt,
	t1.siid_cnt = t2.siid_cnt,
	t1.nonjs_hp_cnt = t2.nonjs_hp_cnt,
	t1.primary_app_id = t2.primary_app_id,
	t1.bot_session = t2.bot_session,
	t1.bot_flags64 = t2.bot_flags64,
	t1.session_flags64 = t2.session_flags64,
	t1.session_traffic_source_id = t2.session_traffic_source_id,
	t1.session_traffic_source_details = t2.session_traffic_source_details,
	t1.first_seqnum = t2.first_seqnum,
	t1.on_ebay_sess = t2.on_ebay_sess,
	t1.session_traffic_source_dtl2 = t2.session_traffic_source_dtl2,
	t1.session_details = t2.session_details,
	t1.ref_domain = t2.ref_domain,
	t1.referrer = t2.referrer,
	t1.roverentry_src_string = t2.roverentry_src_string,
	t1.roverns_src_string = t2.roverns_src_string,
	t1.roveropen_src_string = t2.roveropen_src_string,
	t1.rtm_src_string = t2.rtm_src_string,
	t1.notif_src_string = t2.notif_src_string,
	t1.lndg_page_id = t2.lndg_page_id,
	t1.lndg_page_fmly4 = t2.lndg_page_fmly4,
	t1.lndg_page_url = t2.lndg_page_url,
	t1.lndg_sid = t2.lndg_sid,
	t1.lndg_mppid = t2.lndg_mppid,
	t1.lndg_mnt = t2.lndg_mnt,
	t1.lndg_ort = t2.lndg_ort,
	t1.start_timestamp = t2.start_timestamp,
	t1.end_timestamp = t2.end_timestamp,
	t1.exit_page_id = t2.exit_page_id,
	t1.roverentry_ts = t2.roverentry_ts,
	t1.roverns_ts = t2.roverns_ts,
	t1.roveropen_ts = t2.roveropen_ts,
	t1.rtm_ts = t2.rtm_ts,
	t1.notification_ts = t2.notification_ts,
	t1.notification_id = t2.notification_id,
	t1.debug_flag = t2.debug_flag,
	t1.mcs_entry_src_string = t2.mcs_entry_src_string,
	t1.mcs_entry_ts = t2.mcs_entry_ts,
	t1.updated_ts = t2.updated_ts,
	t1.dlk_entry_src_string = t2.dlk_entry_src_string,
	t1.dlk_entry_ts = t2.dlk_entry_ts,
	t1.dlk_brguid = t2.dlk_brguid,
	t1.dlk_brsess = t2.dlk_brsess,
	t1.dlk_details = t2.dlk_details,
	t1.dlk_deeplink = t2.dlk_deeplink,
	t1.dlk_referrer = t2.dlk_referrer,
	t1.dlk_mweb_link_type = t2.dlk_mweb_link_type,
	t1.entry_event_src_string = t2.entry_event_src_string,
	t1.entry_event_ts = t2.entry_event_ts,
	t1.ns_event_src_string = t2.ns_event_src_string,
	t1.ns_event_ts = t2.ns_event_ts,
	t1.lndg_page_src_string = t2.lndg_page_src_string,
	t1.lndg_page_ts = t2.lndg_page_ts,
	t1.nvts_src_string = t2.nvts_src_string,
	t1.ncts_src_string = t2.ncts_src_string,
	t1.ndts_src_string = t2.ndts_src_string
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt = t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand = t2.cobrand;

DELETE t2
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt = t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand = t2.cobrand;

UPDATE t1
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
SET
	t1.cguid = t2.cguid,
	t1.valid_page_count = t2.valid_page_count,
	t1.min_sc_seqnum = t2.min_sc_seqnum,
	t1.max_sc_seqnum = t2.max_sc_seqnum,
	t1.signedin_user_id = t2.signedin_user_id,
	t1.mapped_user_id = t2.mapped_user_id,
	t1.agent_id = t2.agent_id,
	t1.social_agent_type = t2.social_agent_type,
	t1.search_agent_type = t2.search_agent_type,
	t1.device_type = t2.device_type,
	t1.agent_details = t2.agent_details,
	t1.session_cntry_id = t2.session_cntry_id,
	t1.session_rev_rollup = t2.session_rev_rollup,
	t1.ip = t2.ip,
	t1.gr_cnt = t2.gr_cnt,
	t1.gr_1_cnt = t2.gr_1_cnt,
	t1.vi_cnt = t2.vi_cnt,
	t1.homepage_cnt = t2.homepage_cnt,
	t1.myebay_cnt = t2.myebay_cnt,
	t1.signin_cnt = t2.signin_cnt,
	t1.siid_cnt = t2.siid_cnt,
	t1.nonjs_hp_cnt = t2.nonjs_hp_cnt,
	t1.primary_app_id = t2.primary_app_id,
	t1.bot_session = t2.bot_session,
	t1.bot_flags64 = t2.bot_flags64,
	t1.session_flags64 = t2.session_flags64,
	t1.session_traffic_source_id = t2.session_traffic_source_id,
	t1.session_traffic_source_details = t2.session_traffic_source_details,
	t1.first_seqnum = t2.first_seqnum,
	t1.on_ebay_sess = t2.on_ebay_sess,
	t1.session_traffic_source_dtl2 = t2.session_traffic_source_dtl2,
	t1.session_details = t2.session_details,
	t1.ref_domain = t2.ref_domain,
	t1.referrer = t2.referrer,
	t1.roverentry_src_string = t2.roverentry_src_string,
	t1.roverns_src_string = t2.roverns_src_string,
	t1.roveropen_src_string = t2.roveropen_src_string,
	t1.rtm_src_string = t2.rtm_src_string,
	t1.notif_src_string = t2.notif_src_string,
	t1.lndg_page_id = t2.lndg_page_id,
	t1.lndg_page_fmly4 = t2.lndg_page_fmly4,
	t1.lndg_page_url = t2.lndg_page_url,
	t1.lndg_sid = t2.lndg_sid,
	t1.lndg_mppid = t2.lndg_mppid,
	t1.lndg_mnt = t2.lndg_mnt,
	t1.lndg_ort = t2.lndg_ort,
	t1.start_timestamp = t2.start_timestamp,
	t1.end_timestamp = t2.end_timestamp,
	t1.exit_page_id = t2.exit_page_id,
	t1.roverentry_ts = t2.roverentry_ts,
	t1.roverns_ts = t2.roverns_ts,
	t1.roveropen_ts = t2.roveropen_ts,
	t1.rtm_ts = t2.rtm_ts,
	t1.notification_ts = t2.notification_ts,
	t1.notification_id = t2.notification_id,
	t1.debug_flag = t2.debug_flag,
	t1.mcs_entry_src_string = t2.mcs_entry_src_string,
	t1.mcs_entry_ts = t2.mcs_entry_ts,
	t1.updated_ts = t2.updated_ts,
	t1.dlk_entry_src_string = t2.dlk_entry_src_string,
	t1.dlk_entry_ts = t2.dlk_entry_ts,
	t1.dlk_brguid = t2.dlk_brguid,
	t1.dlk_brsess = t2.dlk_brsess,
	t1.dlk_details = t2.dlk_details,
	t1.dlk_deeplink = t2.dlk_deeplink,
	t1.dlk_referrer = t2.dlk_referrer,
	t1.dlk_mweb_link_type = t2.dlk_mweb_link_type,
	t1.entry_event_src_string = t2.entry_event_src_string,
	t1.entry_event_ts = t2.entry_event_ts,
	t1.ns_event_src_string = t2.ns_event_src_string,
	t1.ns_event_ts = t2.ns_event_ts,
	t1.lndg_page_src_string = t2.lndg_page_src_string,
	t1.lndg_page_ts = t2.lndg_page_ts,
	t1.nvts_src_string = t2.nvts_src_string,
	t1.ncts_src_string = t2.ncts_src_string,
	t1.ndts_src_string = t2.ndts_src_string
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt = t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand <> t2.cobrand AND t1.cobrand in (0,6,7) AND t2.cobrand in (0,6,7);

DELETE t2
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt = t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand <> t2.cobrand AND t1.cobrand in (0,6,7) AND t2.cobrand in (0,6,7);

UPDATE t1
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
SET
	t1.session_start_dt = t2.session_start_dt,
	t1.cguid = t2.cguid,
	t1.valid_page_count = t2.valid_page_count,
	t1.min_sc_seqnum = t2.min_sc_seqnum,
	t1.max_sc_seqnum = t2.max_sc_seqnum,
	t1.signedin_user_id = t2.signedin_user_id,
	t1.mapped_user_id = t2.mapped_user_id,
	t1.agent_id = t2.agent_id,
	t1.social_agent_type = t2.social_agent_type,
	t1.search_agent_type = t2.search_agent_type,
	t1.device_type = t2.device_type,
	t1.agent_details = t2.agent_details,
	t1.session_cntry_id = t2.session_cntry_id,
	t1.session_rev_rollup = t2.session_rev_rollup,
	t1.ip = t2.ip,
	t1.gr_cnt = t2.gr_cnt,
	t1.gr_1_cnt = t2.gr_1_cnt,
	t1.vi_cnt = t2.vi_cnt,
	t1.homepage_cnt = t2.homepage_cnt,
	t1.myebay_cnt = t2.myebay_cnt,
	t1.signin_cnt = t2.signin_cnt,
	t1.siid_cnt = t2.siid_cnt,
	t1.nonjs_hp_cnt = t2.nonjs_hp_cnt,
	t1.primary_app_id = t2.primary_app_id,
	t1.bot_session = t2.bot_session,
	t1.bot_flags64 = t2.bot_flags64,
	t1.session_flags64 = t2.session_flags64,
	t1.session_traffic_source_id = t2.session_traffic_source_id,
	t1.session_traffic_source_details = t2.session_traffic_source_details,
	t1.first_seqnum = t2.first_seqnum,
	t1.on_ebay_sess = t2.on_ebay_sess,
	t1.session_traffic_source_dtl2 = t2.session_traffic_source_dtl2,
	t1.session_details = t2.session_details,
	t1.ref_domain = t2.ref_domain,
	t1.referrer = t2.referrer,
	t1.roverentry_src_string = t2.roverentry_src_string,
	t1.roverns_src_string = t2.roverns_src_string,
	t1.roveropen_src_string = t2.roveropen_src_string,
	t1.rtm_src_string = t2.rtm_src_string,
	t1.notif_src_string = t2.notif_src_string,
	t1.lndg_page_id = t2.lndg_page_id,
	t1.lndg_page_fmly4 = t2.lndg_page_fmly4,
	t1.lndg_page_url = t2.lndg_page_url,
	t1.lndg_sid = t2.lndg_sid,
	t1.lndg_mppid = t2.lndg_mppid,
	t1.lndg_mnt = t2.lndg_mnt,
	t1.lndg_ort = t2.lndg_ort,
	t1.start_timestamp = t2.start_timestamp,
	t1.end_timestamp = t2.end_timestamp,
	t1.exit_page_id = t2.exit_page_id,
	t1.roverentry_ts = t2.roverentry_ts,
	t1.roverns_ts = t2.roverns_ts,
	t1.roveropen_ts = t2.roveropen_ts,
	t1.rtm_ts = t2.rtm_ts,
	t1.notification_ts = t2.notification_ts,
	t1.notification_id = t2.notification_id,
	t1.debug_flag = t2.debug_flag,
	t1.mcs_entry_src_string = t2.mcs_entry_src_string,
	t1.mcs_entry_ts = t2.mcs_entry_ts,
	t1.updated_ts = t2.updated_ts,
	t1.dlk_entry_src_string = t2.dlk_entry_src_string,
	t1.dlk_entry_ts = t2.dlk_entry_ts,
	t1.dlk_brguid = t2.dlk_brguid,
	t1.dlk_brsess = t2.dlk_brsess,
	t1.dlk_details = t2.dlk_details,
	t1.dlk_deeplink = t2.dlk_deeplink,
	t1.dlk_referrer = t2.dlk_referrer,
	t1.dlk_mweb_link_type = t2.dlk_mweb_link_type,
	t1.entry_event_src_string = t2.entry_event_src_string,
	t1.entry_event_ts = t2.entry_event_ts,
	t1.ns_event_src_string = t2.ns_event_src_string,
	t1.ns_event_ts = t2.ns_event_ts,
	t1.lndg_page_src_string = t2.lndg_page_src_string,
	t1.lndg_page_ts = t2.lndg_page_ts,
	t1.nvts_src_string = t2.nvts_src_string,
	t1.ncts_src_string = t2.ncts_src_string,
	t1.ndts_src_string = t2.ndts_src_string
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt <> t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand = t2.cobrand;

DELETE t2
FROM
	p_soj_cl_t.temp_csess3a_v9 t1,
	p_soj_cl_t.temp_csess3b_v9 t2
WHERE
	t1.guid = t2.guid AND t1.session_skey = t2.session_skey AND t1.session_start_dt = t2.session_start_dt AND t1.site_id = t2.site_id AND t1.cobrand = t2.cobrand;

INSERT INTO p_soj_cl_t.temp_csess3a_v9
(
	guid,
	session_skey,
	site_id,
	cobrand,
	cguid,
	valid_page_count,
	min_sc_seqnum,
	max_sc_seqnum,
	signedin_user_id,
	mapped_user_id,
	agent_id,
	social_agent_type,
	search_agent_type,
	device_type,
	agent_details,
	session_cntry_id,
	session_rev_rollup,
	ip,
	gr_cnt,
	gr_1_cnt,
	vi_cnt,
	homepage_cnt,
	myebay_cnt,
	signin_cnt,
	siid_cnt,
	nonjs_hp_cnt,
	primary_app_id,
	bot_session,
	bot_flags64,
	session_flags64,
	session_traffic_source_id,
	session_traffic_source_details,
	first_seqnum,
	on_ebay_sess,
	session_traffic_source_dtl2,
	session_details,
	ref_domain,
	referrer,
	roverentry_src_string,
	roverns_src_string,
	roveropen_src_string,
	rtm_src_string,
	notif_src_string,
	lndg_page_id,
	lndg_page_fmly4,
	lndg_page_url,
	lndg_sid,
	lndg_mppid,
	lndg_mnt,
	lndg_ort,
	ref_keyword,
	start_timestamp,
	end_timestamp,
	exit_page_id,
	roverentry_ts,
	roverns_ts,
	roveropen_ts,
	rtm_ts,
	notification_ts,
	notification_id,
	debug_flag,
	mcs_entry_src_string,
	mcs_entry_ts,
	updated_ts,
	dlk_entry_src_string,
	dlk_entry_ts,
	dlk_brguid,
	dlk_brsess,
	dlk_details,
	dlk_deeplink,
	dlk_referrer,
	dlk_mweb_link_type,
	entry_event_src_string,
	entry_event_ts,
	ns_event_src_string,
	ns_event_ts,
	session_start_dt,
	lndg_page_src_string,
	lndg_page_ts,
	nvts_src_string,
	ncts_src_string,
	ndts_src_string
)
SELECT
	guid,
	session_skey,
	site_id,
	cobrand,
	cguid,
	valid_page_count,
	min_sc_seqnum,
	max_sc_seqnum,
	signedin_user_id,
	mapped_user_id,
	agent_id,
	social_agent_type,
	search_agent_type,
	device_type,
	agent_details,
	session_cntry_id,
	session_rev_rollup,
	ip,
	gr_cnt,
	gr_1_cnt,
	vi_cnt,
	homepage_cnt,
	myebay_cnt,
	signin_cnt,
	siid_cnt,
	nonjs_hp_cnt,
	primary_app_id,
	bot_session,
	bot_flags64,
	session_flags64,
	session_traffic_source_id,
	session_traffic_source_details,
	first_seqnum,
	on_ebay_sess,
	session_traffic_source_dtl2,
	session_details,
	ref_domain,
	referrer,
	roverentry_src_string,
	roverns_src_string,
	roveropen_src_string,
	rtm_src_string,
	notif_src_string,
	lndg_page_id,
	lndg_page_fmly4,
	lndg_page_url,
	lndg_sid,
	lndg_mppid,
	lndg_mnt,
	lndg_ort,
	ref_keyword,
	start_timestamp,
	end_timestamp,
	exit_page_id,
	roverentry_ts,
	roverns_ts,
	roveropen_ts,
	rtm_ts,
	notification_ts,
	notification_id,
	debug_flag,
	mcs_entry_src_string,
	mcs_entry_ts,
	updated_ts,
	dlk_entry_src_string,
	dlk_entry_ts,
	dlk_brguid,
	dlk_brsess,
	dlk_details,
	dlk_deeplink,
	dlk_referrer,
	dlk_mweb_link_type,
	entry_event_src_string,
	entry_event_ts,
	ns_event_src_string,
	ns_event_ts,
	session_start_dt,
	lndg_page_src_string,
	lndg_page_ts,
	nvts_src_string,
	ncts_src_string,
	ndts_src_string
FROM
	p_soj_cl_t.temp_csess3b_v9 t2i;

UPDATE s
FROM
	p_soj_cl_t.temp_csess3a_v9 s,
	(
		SELECT
			s1.guid,
			s1.session_skey,
			s1.session_start_dt,
			s1.site_id,
			s1.cobrand
		FROM
			p_soj_cl_t.temp_csess3a_v9 s1
			INNER JOIN (
							SELECT
								guid,
								session_skey,
								session_start_dt
							FROM
								ubi_v.ubi_session
							WHERE
								session_start_dt BETWEEN '2024-05-30' AND '2024-06-01' AND bot_flag > 0 AND bot_flag NOT IN (12,15)
							GROUP BY 1,2,3
						) s2
			ON (s1.guid = s2.guid AND s1.session_skey = s2.session_skey AND s1.session_start_dt = s2.session_start_dt)
			WHERE
				s1.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01'
				AND (s1.bot_flags64 IS NULL OR ((s1.bot_flags64 & 1) = 0)) /* not currently flagged AS UBI BOT */
	) bot
SET
	s.bot_session = 1,
	s.bot_flags64 = s.bot_flags64 + 1
WHERE
	s.guid = bot.guid AND s.session_skey = bot.session_skey AND s.session_start_dt = bot.session_start_dt AND s.site_id = bot.site_id AND s.cobrand = bot.cobrand AND s.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01';

UPDATE t3a
FROM
	p_soj_cl_t.temp_csess3a_v9 t3a,
	p_soj_cl_t.session_guid_uid_map b
SET
	t3a.mapped_user_id = b.mapped_uid
WHERE
	t3a.guid = b.guid AND t3a.session_start_dt = b.session_start_dt AND t3a.signedin_user_id IS NULL AND t3a.mapped_user_id IS NULL AND b.session_start_dt BETWEEN '2024-05-30' AND '2024-06-01';

UPDATE t3a
FROM
	p_soj_cl_t.temp_csess3a_v9 t3a,
	access_views.dw_users par
SET
	t3a.parent_uid = par.prmry_user_id
WHERE
	COALESCE(t3a.signedin_user_id,t3a.mapped_user_id) = par.user_id and t3a.parent_uid IS NULL;