#!/bin/bash

# cleanup backup directories in /sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src
# cleanup t-1 sameday and t-2 crossday
# cleanup is required before rerun the swap job or the whole pipeline

# Warning: change the dt partition first
dt_2=20230317
dt_1=20230318

hadoop fs -rm -r -skipTrash /sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_2}/data_type=major/session_type=crossday
hadoop fs -rm -r -skipTrash /sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_2}/data_type=patch/session_type=crossday
hadoop fs -rm -r -skipTrash /sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=major/session_type=sameday
hadoop fs -rm -r -skipTrash /sys/edw/ubi/ubi_t/soj/unified_session_swap/v1_no_trfc_src/dt=${dt_1}/data_type=patch/session_type=sameday
