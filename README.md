# WDPS2019

1) Log in on DAS4

2) $cd WDPS2019

3) $source .env/bin/activate

4) $python wdps.py

prun -np 1 -t $TIME $SPARK_HOME/bin/spark-submit \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./ENV/bin/python3 \
    --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    --master local[16] \
    --executor-memory 8G \
    --driver-memory 8G \
    --num-executors 16 \
    --archives venv.zip#ENV \
    run.py $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT $INPUT $OUTPUT