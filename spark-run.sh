#!/bin/bash

cd ${SPARK_HOME}

export MASTER="spark://$SPARK_MASTER_NODE:$SPARK_MASTER_PORT"

projhome="/ccs/techint/proj/spider2-snapshot-analysis/"

#pyscript="${projhome}/scripts/wordcount.py"
#pyscript="${projhome}/scripts/schema.py"
pyscript="${projhome}/scripts/anonymize.py"

command="bin/spark-submit --master ${MASTER} "
command+="--py-files ${pyscript} ${pyscript} 20151019"

echo "Running $command" >&2
$command

