#!/bin/bash
#-------------------------------------------------------------#
if [ "$1" == "0" ]; then
    sbt clean package

#-------------------------------------------------------------#
elif [ "$1" == "1" ]; then
spark-submit    \
  --class "TopDomains"      \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 16G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar $2 $3 
  

elif [ "$1" == "2" ]; then
spark-submit    \
  --class "TotalLinks"      \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 16G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar  

#-------------------------------------------------------------#
else

  echo "+---------------------------------------------"
  echo "| 0. Build Assembly                           "
  echo "| 1. ./run.sh 1 vdomain 750                   "
  echo "| 2. Total Links                              "
  echo "+---------------------------------------------"
  echo "| 4. Clear                                    "
  echo "+---------------------------------------------"

fi

