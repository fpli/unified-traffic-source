#!/usr/bin/ksh -e
#------------------------------------------------------------------------------------------------
# Title:
# File Name:    sg_ubi_data.tracking_events_daily_same_day_done_check.ksh
# Description:  check if same_day done file is generated
# Location:     Apollo-devour: /export/home/o_ubi/etl/shell/sg_ubi_data.tracking_events_daily_same_day_done_check.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Lu Dongfang      2023-01-18      Initial Creation
# Lu Dongfang      2023-02-08      Using UOW_FROM
#------------------------------------------------------------------------------------------------


run_dt="${UOW_FROM:0:4}-${UOW_FROM:4:2}-${UOW_FROM:6:2}"

export HDP_CMD="/apache/hadoop/bin/hadoop"

re_run=0

for i in {1..60}
do
  re_run=0
  folder="/apps/b_trk/tracking_events_daily/dt=${run_dt}/_PARTLY_SUCCESS_ADD_PARTITION"
  current_time=`date '+%Y-%m-%d %H:%M:%S'`
  echo "current time: $current_time"
  set +e
    dt=`${HDP_CMD} fs -ls $folder`
    rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    echo "failed to list information for folder: $folder"
    re_run=1
  fi

  if [ $re_run != 0 ]
  then
    echo "done file not ready, please wait for 3 mins"
    sleep 180
  else
    echo "done file ready, please going on."
    exit 0
  fi

done
exit -1
