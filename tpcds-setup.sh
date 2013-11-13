#!/bin/bash

mkdir ~/tpcds
cd ~/tpcds
curl --output tpcds_kit.zip http://www.tpc.org/tpcds/dsgen/dsgen-download-files.asp?download_key=NaN
unzip tpcds_kit.zip

cd tools
make clean
make

# generate the data
export PATH=$PATH:.
DIR=$HOME/tpcds/data
mkdir -p $DIR
SCALE=1000
FORCE=Y

hdfs dfs -rm -r /hive/tpcds
# copy data to hdfs
hdfs dfs -mkdir /hive/tpcds
hdfs dfs -mkdir /hive/tpcds/date_dim
hdfs dfs -mkdir /hive/tpcds/time_dim
hdfs dfs -mkdir /hive/tpcds/item
hdfs dfs -mkdir /hive/tpcds/customer
hdfs dfs -mkdir /hive/tpcds/customer_demographics
hdfs dfs -mkdir /hive/tpcds/household_demographics
hdfs dfs -mkdir /hive/tpcds/customer_address
hdfs dfs -mkdir /hive/tpcds/store
hdfs dfs -mkdir /hive/tpcds/promotion
hdfs dfs -mkdir /hive/tpcds/store_sales

function putAndPurge {
  for t in $DIR/$1
  do
    hdfs dfs -put ${t}.dat /hive/tpcds/${t}
  done
  rm -rfv $DIR/$1
}

dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table store_sales
putAndPurge store_sales
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table date_dim
putAndPurge date_dim
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table time_dim
putAndPurge time_dim
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table item
putAndPurge item
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table customer
putAndPurge customer
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table customer_demographics
putAndPurge customer_demographics
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table household_demographics
putAndPurge household_demographics
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table customer_address
putAndPurge customer_address
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table store
putAndPurge store
dsdgen -verbose -force $FORCE -dir $DIR -scale $SCALE -table promotion
putAndPurge promotion

hdfs dfs -ls -R /hive/tpcds/*/*.dat

# create the tables via hive
#hive -f ~/impalascripts/tpcds_ss_tables.sql
