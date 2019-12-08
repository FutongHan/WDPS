# Activate the virtual env
source .env/bin/activate
module load prun
export SPARK_HOME="/home/bbkruit/spark-2.4.0-bin-without-hadoop"
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Start Elasticsearch server
ES_PORT=9200
ES_BIN=/home/wdps1911/prof/wdps/elasticsearch-2.4.1/bin/elasticsearch

>.es_log*
prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

# TODO: Start Trident server
############################
KB_PORT=9090
KB_BIN=/home/jurbani/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

echo "Lauching an instance of the Trident server on a random node in the cluster ..."
prun -o .kb_log -v -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "Waiting 5 seconds for trident to set up (use 'preserve -llist' to see if the node has been allocated)"
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
KB_PID=$!
echo "Trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"

# Entity recognition and linking
# python run.py $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT

spark-submit \
--master local[5]
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 5 \
run_spark.py $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT

# Stop Trident server
kill $KB_PID

# Stop Elasticsearch server
kill $ES_PID

# Deactivate virtual env
deactivate