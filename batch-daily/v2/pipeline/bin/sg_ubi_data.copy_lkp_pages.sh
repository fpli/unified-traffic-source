#!/usr/bin/env bash

set -x
export HDP_CMD="/apache/hadoop/bin/hadoop"

cnt=$($HDP_CMD fs -find /sys/edw/working/ubi/ubi_w/soj/uts_v2_lkp_pages -name '*.avro' | wc -l)

if [ $cnt -eq 1 ] ; then
  echo "There is exactly 1 Avro file. Copy it to target."
  src=$($HDP_CMD fs -find /sys/edw/working/ubi/ubi_w/soj/uts_v2_lkp_pages -name '*.avro')
  dst="/sys/soj/ubd/traffic-source/lookup/lkp_pages.avro"
  $HDP_CMD fs -cp -f $src $dst
  echo "Copied from $src to $dst"
else
  echo "Not exactly 1 Avro file. Exit."
  exit -1
fi
