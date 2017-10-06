#!/bin/bash
#-------------------------------------------------------------------#
if [ "$1" == "1" ]; then
spark-submit    \
  --class "Domains"         \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar vdomain 1000 
  

elif [ "$1" == "2" ]; then
spark-submit    \
  --class "Links"           \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar  

#-------------------------------------------------------------------#
else

  echo "+---------------------------------------------"
  echo "| 1. Health Domains                           "
  echo "| 2. Total Links                              "
  echo "+---------------------------------------------"

fi
#-------------------------------------------------------------------#

