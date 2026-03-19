#!/bin/bash

hdfs_upload() {
    hdfs_user_name="$1"
    local_file_path="$2"
    hdfs_file_path="$3"
    curl -# \
        --verbose \
        -L \
        -X PUT \
        -H Content-Type:application/octet-stream \
        -T "$local_file_path" \
        "http://webhdfs-offline-lt.corp.kuaishou.com/webhdfs/v1$hdfs_file_path?op=CREATE&data=true&user.name=$hdfs_user_name&overwrite=true"
}

blaze_jar_241kwaiae="$(basename target/blaze-engine-spark-241kwaiae-release-*.jar)"
blaze_jar_35="$(basename target/blaze-engine-spark-3.5-release-*.jar)"

if [ ! -f "target/$blaze_jar_241kwaiae" ]; then
    echo "builded jar not found: target/$blaze_jar_241kwaiae"
    exit 1
fi
if [ ! -f "target/$blaze_jar_35" ]; then
    echo "builded jar not found: target/$blaze_jar_35"
    exit 1
fi

hdfs_staging_dir="/home/spark/spark_jars/blaze/staging"
hdfs_release_dir="/home/spark/spark_jars/blaze"

hdfs_upload spark "target/$blaze_jar_241kwaiae" "$hdfs_staging_dir/$blaze_jar_241kwaiae"
hdfs_upload spark "target/$blaze_jar_35" "$hdfs_staging_dir/$blaze_jar_35"
