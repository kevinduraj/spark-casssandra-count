#!/bin/bash
#----------------------------------------------------#
sbt clean package

spark-submit    \
  --class "TotalLinks"      \
  --master local[16]        \
  --driver-memory   32G     \
  --executor-memory 16G     \
  target/scala-2.11/spark-cassandra-count_2.11-1.0.jar  

#----------------------------------------------------#
