#!/bin/bash

sbt clean package

spark-submit    \
  --class "TotalLinksInDallas"  \
  --master local[8]             \
  --driver-memory   32G         \
  --executor-memory 16G         \
  target/scala-2.11/spark-cassandra_2.11-1.0.jar 

