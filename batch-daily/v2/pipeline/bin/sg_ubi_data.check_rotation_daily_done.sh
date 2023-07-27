#!/usr/bin/env bash

# set -x
export HDP_CMD="/apache/hadoop/bin/hadoop"

done_file_dt=${dt_1}
done_file_path="viewfs://apollo-rno/apps/b_marketing_tracking/watch/${done_file_dt}/rotation_daily.done.${done_file_dt}"
echo "Done file: ${done_file_path}"

echo "Check done file on Apollo-RNO"
${HDP_CMD} fs -test -e ${done_file_path}

while [ $? -ne 0 ]; do
    echo "Done file on Apollo-RNO not generated! Keep waiting..."
    sleep 60
    ${HDP_CMD} fs -test -e ${done_file_path}
done

echo "Done file on Apollo-RNO generated."
