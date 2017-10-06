#!/bin/bash
#-------------------------------------------------------------------#
if [ "$1" == "1" ]; then
spark-submit    \
  --class "Domains"         \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar vdomain \
  /root/spark-cassandra-count/data/diseases.dat 

  
elif [ "$1" == "2" ]; then
spark-submit    \
  --class "Links"           \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar  

elif [ "$1" == "3" ]; then
spark-submit    \
  --class "Files"           \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar \
  /root/spark-cassandra-count/data/diseases.dat 

elif [ "$1" == "4" ]; then
spark-submit    \
  --class "ExportLinks"     \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 32G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar links \
  /root/spark-cassandra-count/data/health.dat 

#-------------------------------------------------------------------#
else

  echo "+-----------------------------------------------+"
  echo "|  1. Retrieve Health Domains to cloud2.health  |"
  echo "|  2. Total Links                               |"
  echo "|  3. Reading File                              |"
  echo "|  4. Export Health Links                       |"
  echo "+-----------------------------------------------+"

fi
#-------------------------------------------------------------------#

