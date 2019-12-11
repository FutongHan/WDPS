# Activate the virtual env
source .env/bin/activate
module load prun

PYSPARK_PYTHON=./.env/bin/python3
/home/bbkruit/spark-2.4.0-bin-without-hadoop/bin/spark-submit \
--num-executors 16 \
--executor-memory 4G \
pi.py

# Deactivate virtual env
deactivate
