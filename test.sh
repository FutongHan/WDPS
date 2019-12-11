# Activate the virtual env
source .env/bin/activate
module load prun

PYSPARK_PYTHON=./.env/bin/python3
/home/bbkruit/spark-2.4.0-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.env/bin/python3 \
--conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--master yarn \
--deploy-mode cluster \
--num-executors 16 \
--executor-memory 4G \
test.py

# Deactivate virtual env
deactivate