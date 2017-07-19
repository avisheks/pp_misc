#!/usr/bin/env bash


export LOCAL=0


# set params
OUTDIR="/tmp/avishek2/output_amit"
OUTPUT="${OUTDIR}/qlas_cat_w2v"
MIN_CLUSTERS=25
MAX_CLUSTERS=25
MAX_OUT_PARTITIONS=100
SAMPLING_RATIO=0.1


# ====================================================================================
if [[ ${LOCAL} == 1 ]];
then
    #
    INPUT="/Users/avishek2/pet_projects/pp_misc/clustering/res/data/small_input_100.txt"
    #
    export SPARK_HOME="/Users/avishek2/pub/opt/spark"
    export SPARK_LOCAL_IP="127.0.0.1"
    export SPARK_MASTER="local[2]"
    #
else
    #
    #QLAS_INPUT="/projects/qlas/categorization_model/model_validation2/travel/part*"
    #W2V_INPUT="/projects/cb_antares/curveball_prod/antares/w2v/20170201/model/part*"
    INPUT="/projects/qlas/categorization_model/model_result_wrod2vec/travel/part*"
    #
    export SPARK_HOME="/home/gs/spark/latest"
    export SPARK_CONF_DIR="/home/gs/conf/spark/latest"
    export SPARK_YARN_MODE="true"
    export SPARK_MASTER="yarn"
    #
fi


# ====================================================================================
time \
hadoop fs -rm -r -skipTrash ${OUTPUT}*
./run_generic.sh \
    clustering_amit.py \
    -q ${INPUT} \
    -o ${OUTPUT} \
    -m ${MIN_CLUSTERS} \
    -k ${MAX_CLUSTERS} \
    -p ${MAX_OUT_PARTITIONS} \
    -s ${SAMPLING_RATIO}

hadoop fs -chmod -R 755 ${OUTDIR}