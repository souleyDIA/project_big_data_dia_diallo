#!/bin/bash

# Start the docker-compose
echo "Waiting for the containers to start..."
docker-compose up -d
echo "Containers are started."

echo "Copy the Reviews.csv file to the namenode container"
# Copy the Reviews.csv file to the namenode container
docker cp ./Reviews.csv project-big-data-dia-diallo-namenode-1:/tmp/

echo "Copy the Spark script and Dash app to the spark-master container"
# Copy the Spark script and Dash app to the spark-master container 
docker cp ./spark_processing.py project-big-data-dia-diallo-spark-master-1:/tmp/
docker cp ./dash_app.py project-big-data-dia-diallo-spark-master-1:/tmp/

# Check if datanode is connected to namenode
# This loop ensures that HDFS commands are executed only when the datanode is connected to the namenode, 
# preventing potential errors due to unestablished connections.
echo "Waiting for datanode to connect to namenode..."
while true; do
    DATANODE_STATUS=$(docker exec project-big-data-dia-diallo-namenode-1 hdfs dfsadmin -report 2>&1 | grep "Live datanodes")
    echo "DATANODE_STATUS: $DATANODE_STATUS" # Pour le débogage, vérifier le statut du datanode
    if [[ $DATANODE_STATUS == *"Live datanodes (1):"* ]]; then
        break
    else
        echo "Datanode not yet connected. Retrying in 10 seconds..."
        sleep 10
    fi
done
echo "Datanode is connected to namenode."


echo "Install dependencies for Dash in spark master"
# To be executed in spark-master
docker exec -it project-big-data-dia-diallo-spark-master-1 bash -c "
pip install dash dash-core-components dash-html-components dash-renderer dash-bootstrap-components pandas plotly pyspark findspark textblob
"
echo "Dependencies installed"

echo "Execute HDFS commands to prepare the environment for data processing"
# Connect to the namenode container and execute HDFS commands
# We need to grant write permisions for the user spark to be able to put the 
# processed data in the HDFS 
# Because I had this error when I lauched the script:
# Permission denied: user=spark, access=WRITE, inode="/":hadoop:supergroup:drwxr-xr-x 
docker exec -it project-big-data-dia-diallo-namenode-1 bash -c "
hdfs dfs -mkdir -p /data &&
hdfs dfs -put /tmp/Reviews.csv /data/ &&
hdfs dfs -mkdir -p /results &&
hdfs dfs -chown spark:spark /results &&
hdfs dfs -chmod 755 /results
"

echo "HDFS commands executed"

echo "Execute script for data processing and Dash app"

# Execute script for data processing
# To be executed in spark-master
docker exec -it project-big-data-dia-diallo-spark-master-1 /opt/bitnami/spark/bin/spark-submit /tmp/spark_processing.py

# Execute script for the Dash app
# To be executed in spark-master
docker exec -it project-big-data-dia-diallo-spark-master-1 /opt/bitnami/spark/bin/spark-submit /tmp/dash_app.py

echo "Script executed"
echo "Open your browser and go to http://localhost:8050/ to see the Dash app"
