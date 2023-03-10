i#!/usr/bin/ksh -e
#------------------------------------------------------------------------------------------------
# Title:
# File Name:    sg_ubi_data.unified_session_without_ts_done_check.ksh
# Description:  check if unified session without traffic source done file is generated
# Location:     ETL Server: /home/cmao/etl/shell/sg_ubi_data.unified_session_without_ts_done_check.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Mao Chenxiao      2023-03-02      Initial Creation
#------------------------------------------------------------------------------------------------

echo "Check variables:"
echo "UOW_FROM: ${UOW_FROM}"
echo "UOW_TO: ${UOW_TO}"
echo "dt_1: ${dt_1}"
echo "dt_1_formated: ${dt_1_formated}"
echo "dt_2: ${dt_2}"
echo "dt_2_formated: ${dt_2_formated}"

/dw/etl/mstr_bin/dw_infra.watch_file_check.ksh -i sg_ubi_data.dummy_etl_id -e hd10 -f ${UOW_TO} -t ${UOW_TO} -j hd10 -w sg_ubi_data.ubi_t.unified_session_without_ts.done
rcode=$?
exit $rcode
