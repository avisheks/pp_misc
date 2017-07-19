#!/usr/bin/env bash


if [ -z ${SPARK_MASTER} ]; then SPARK_MASTER="local[2]"; fi


export PYSPARK_DRIVER_PYTHON="./anaconda2/bin/python"
export PYSPARK_PYTHON="./anaconda2/bin/python"


time\
SPARK_SUBMIT_OPTS="-Dspark.shuffle.memoryFraction=0.75 \
    -Dspark.storage.memoryFraction=0.05" \
    ${SPARK_HOME}/bin/spark-submit \
    --master ${SPARK_MASTER} \
    --driver-memory 6G \
    --executor-memory 6G \
    --queue default \
    --num-executors 800 \
    --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON} \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON} \
    --conf spark.yarn.appMasterEnv.PYTHONPATH=./anaconda/ \
    --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./anaconda/lib/ \
    --conf spark.yarn.executorEnv.PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON} \
    --conf spark.yarn.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON} \
    --conf spark.yarn.executorEnv.PYTHONPATH=./anaconda/ \
    --conf spark.yarn.executorEnv.LD_LIBRARY_PATH=./anaconda/lib/ \
    --archives 'hdfs://jetblue-nn1.blue.ygrid.yahoo.com/user/avishek2/archives/anaconda.zip#anaconda2' \
    --py-files clustering.py \
$*