#!/bin/bash

SPARK_APPLICATION_JAR_LOCATION=`find /app/target -iname '*.jar' | head -n1`
export SPARK_APPLICATION_JAR_LOCATION

if [ -z "$SPARK_APPLICATION_JAR_LOCATION" ]; then
	echo "Can't find a file *-assembly-*.jar in /app/target"
	exit 1
fi

rm -rf /tmp/data/graph.json


SPARK_HOME=/usr/local/spark

#${SPARK_HOME}/bin/run-example SparkPi

${SPARK_HOME}/bin/spark-submit \
  --class "Main" \
  --master spark://master:7077 \
  "${SPARK_APPLICATION_JAR_LOCATION}"

rm -rf /tmp/data/bootstrap

python graph-visual.py

if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi

if [[ $1 == "-bash" ]]; then
  /bin/bash
fi

