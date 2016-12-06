#!/bin/bash

indir=$1
outdir=$2

$SPARK_HOME/bin/spark-submit \
  --class "vu.wdps.group09.EntityVectorExtractor" \
  --master "local[6]" \
  --jars /home/wdps1609/stanford-corenlp-caseless-2014-02-25-models.jar \
  EntityVectorExtractor-assembly-1.0.jar $indir $outdir