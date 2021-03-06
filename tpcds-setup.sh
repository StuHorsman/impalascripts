#!/bin/bash

if [ "$#" -eq 0 ]; then
  echo "Usage: tpcds-setup.sh BASE_DIR SCALE_FACTOR(optional)"
  exit
fi

SCALE=1
if [ "$#" -eq 2 ]; then
  SCALE=$2
fi

BASE_DIR=$1

mkdir $BASE_DIR/tpcds
cd $BASE_DIR/tpcds
curl --output tpcds_kit.zip http://www.tpc.org/tpcds/dsgen/dsgen-download-files.asp?download_key=NaN
unzip tpcds_kit.zip

cd tools
make clean
make

# generate the data
export PATH=$PATH:.
DIR=$BASE_DIR/tpcds/data
mkdir -p $DIR
FORCE=Y
PARALLEL=8

hdfs dfs -rm -r /user/hive/tpcds
# copy data to hdfs
hdfs dfs -mkdir /user/hive
hdfs dfs -mkdir /user/hive/tpcds
hdfs dfs -mkdir /user/hive/tpcds/date_dim
hdfs dfs -mkdir /user/hive/tpcds/time_dim
hdfs dfs -mkdir /user/hive/tpcds/item
hdfs dfs -mkdir /user/hive/tpcds/customer
hdfs dfs -mkdir /user/hive/tpcds/customer_demographics
hdfs dfs -mkdir /user/hive/tpcds/household_demographics
hdfs dfs -mkdir /user/hive/tpcds/customer_address
hdfs dfs -mkdir /user/hive/tpcds/store
hdfs dfs -mkdir /user/hive/tpcds/promotion
hdfs dfs -mkdir /user/hive/tpcds/store_sales

function putAndPurge {
  for t in $DIR/$1*.dat
  do
    hdfs dfs -put $t /user/hive/tpcds/$1
    rm -fv $t
  done
}

dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -parallel $PARALLEL
putAndPurge store_sales
putAndPurge date_dim
putAndPurge time_dim
putAndPurge item
putAndPurge customer_demographics
putAndPurge household_demographics
putAndPurge customer_address
putAndPurge customer
putAndPurge store
putAndPurge promotion

hdfs dfs -ls -R /user/hive/tpcds/*/*.dat

# create the tables via /user/hive
hive -f ./tpcds_ss_tables.sql
