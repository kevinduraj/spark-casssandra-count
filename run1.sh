#!/bin/bash
#-------------------------------------------------------------#
if [ "$1" == "0" ]; then
    sbt clean package

#-------------------------------------------------------------#
elif [ "$1" == "1" ]; then
spark-submit    \
  --class "TotalLinks"      \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 16G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar  

#-------------------------------------------------------------#
else

  echo "+--------------------------------"
  echo "| 0. Build Assembly              "
  echo "| 1. Total Links                 "
  echo "+--------------------------------"
  echo "| 4. Clear                       "
  echo "+--------------------------------"

fi
