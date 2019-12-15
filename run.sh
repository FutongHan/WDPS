# Set up
source .env/bin/activate
virtualenv --relocatable .env
module load prun
module load hadoop
export SPARK_HOME=/home/wdps1911/spark
export SPARK_LOCAL_DIRS=/home/wdps1911/tmp
export PYSPARK_PYTHON=/home/wdps1911/WDPS2019/.env/bin/python3
export YARN_CONF_DIR=/cm/shared/package/hadoop/hadoop-2.7.6/etc/hadoop

TIME=30:00

INPUT=${1:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTPUT=${2:-"output"}

hdfs dfs -rm -r $OUTPUT
#zip -r venv.zip .env

# Start Elasticsearch server
############################
ES_PORT=9200
ES_BIN=/home/wdps1911/prof/wdps/elasticsearch-2.4.1/bin/elasticsearch

>.es_log*
prun -o .es_log -v -np 1 -t $TIME ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

# Start Trident server
############################
KB_PORT=9090
KB_BIN=/home/jurbani/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

echo "Lauching an instance of the Trident server on a random node in the cluster ..."
prun -o .kb_log -v -np 1 -t $TIME $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "Waiting 5 seconds for trident to set up (use 'preserve -llist' to see if the node has been allocated)"
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
KB_PID=$!
echo "Trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"

# Start Spark and the Entity Recognition + Entity Linking process
############################

prun -np 1 -t $TIME $SPARK_HOME/bin/spark-submit \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./ENV/bin/python3 \
    --master local[16] \
    --executor-memory 4G \
    --num-executors 16 \
    --archives venv.zip#ENV \
    run.py $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT $INPUT $OUTPUT

hdfs dfs -copyToLocal $OUTPUT $OUTPUT

# Stop Trident server
kill $KB_PID

# Stop Elasticsearch server
kill $ES_PID

# Deactivate virtual env
deactivate
