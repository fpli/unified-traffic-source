#!/usr/bin/ksh -e
#------------------------------------------------------------------------------------------------
# Title:
# File Name:    sg_ubi_data.swap_unified_session_with_trfc_src.ksh
# Description:  Swap unified session between partitions with traffic source and partitions without traffic source
# Location:     Apollo-devour: /export/home/o_ubi/etl/shell/sg_ubi_data.swap_unified_session_with_trfc_src.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Mao Chenxiao      2023-03-06      Initial Creation
# Mao Chenxiao      2023-05-23      Support open session
#------------------------------------------------------------------------------------------------
echo "Check variables:"
echo "UOW_FROM: ${UOW_FROM}"
echo "UOW_TO: ${UOW_TO}"
echo "dt_1: ${dt_1}"
echo "dt_1_formated: ${dt_1_formated}"
echo "dt_2: ${dt_2}"
echo "dt_2_formated: ${dt_2_formated}"

# For testing only
# export dt_2=20230305
# export dt_1=20230306

export HDP_CMD="/apache/hadoop/bin/hadoop"

# usage:
# create_dir_if_not_exist <dir_to_create>
create_dir_if_not_exist ()
{
  set +e
    $HDP_CMD fs -ls -d $1
    rcode=$?
  set -e
  if [ $rcode != 0 ]
  then
    echo "directory $1 does not exist. create it."
    $HDP_CMD fs -mkdir -p $1
  else
    echo "directory $1 already exists. do nothing."
  fi
}

# usage:
# swap_partition <target_x> <target_x_parent> <swap_x> <swap_x_parent>
swap_partition ()
{
  echo "target: $1"
  echo "target parent: $2"
  echo "swap: $3"
  echo "swap parent: $4"
  set +e
    $HDP_CMD fs -ls -d $1
    rcode=$?
  set -e
  if [ $rcode != 0 ]
  then
    echo "partition folder $1 does not exist. do nothing."
  else
    echo "partition folder $1 exist. swap."
    echo "move $1 to $4"
    $HDP_CMD fs -mv $1 $4
    echo "move $3 to $2"
    $HDP_CMD fs -mv $3 $2
  fi
}

# target table folders and parent folders
target_1=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_2}/data_type=major/session_type=crossday
target_2=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_2}/data_type=patch/session_type=crossday
target_3=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=major/session_type=sameday
target_4=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=patch/session_type=sameday
target_5=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=major/session_type=open
target_6=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=patch/session_type=open

target_1_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_2}/data_type=major
target_2_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_2}/data_type=patch
target_3_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=major
target_4_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=patch
target_5_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=major
target_6_parent=/sys/edw/ubi/ubi_t/soj/unified_session/snapshot/dt=${dt_1}/data_type=patch

# swap table folders and intermediate parent folders
swap_1=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_2}/data_type=major/session_type=crossday
swap_2=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_2}/data_type=patch/session_type=crossday
swap_3=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_1}/data_type=major/session_type=sameday
swap_4=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_1}/data_type=patch/session_type=sameday
swap_5=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_1}/data_type=major/session_type=open
swap_6=/sys/edw/ubi/ubi_t/soj/unified_session_swap/snapshot/dt=${dt_1}/data_type=patch/session_type=open

swap_1_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_2}/data_type=major
swap_2_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_2}/data_type=patch
swap_3_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=major
swap_4_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=patch
swap_5_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=major
swap_6_parent=/sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=patch

# create intermediate parent folders for swap
create_dir_if_not_exist $swap_1_parent
create_dir_if_not_exist $swap_2_parent
create_dir_if_not_exist $swap_3_parent
create_dir_if_not_exist $swap_4_parent
create_dir_if_not_exist $swap_5_parent
create_dir_if_not_exist $swap_6_parent

# swap folders
swap_partition $target_1 $target_1_parent $swap_1 $swap_1_parent
swap_partition $target_2 $target_2_parent $swap_2 $swap_2_parent
swap_partition $target_3 $target_3_parent $swap_3 $swap_3_parent
swap_partition $target_4 $target_4_parent $swap_4 $swap_4_parent
swap_partition $target_5 $target_5_parent $swap_5 $swap_5_parent
swap_partition $target_6 $target_6_parent $swap_6 $swap_6_parent
