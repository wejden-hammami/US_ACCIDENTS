#!/bin/bash
sqoop import \
  --connect jdbc:mysql://mysql:3306/accidents_db \
  --username root \
  --password root \
  --table accidents \
  --target-dir /data/us_accidents/raw/sqoop \
  --delete-target-dir \
  --m 1 \
  --fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N'

hdfs dfs -ls /data/us_accidents/raw/sqoop/
hdfs dfs -count /data/us_accidents/raw/sqoop/
