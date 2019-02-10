MongoDB.local: MongoDB Spark Connector
======================================

In this demo we will be setting up a network word counter. 

Requirements
------------

* MongoDB 4.0+
* NodeJS 9+
* Apache Spark 4.2+


Steps:
------

- Install Spark 

  .. code-block:: sh
    
    mkdir /tmp/demo-spark-mongodb-talk
    cd !$
    wget https://www-eu.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
    tar xzvf spark-2.4.0-bin-hadoop2.7.tgz 

- Run local MongoDB server 

  .. code-block:: sh 
    
    mkdir db 
    mongod --dbpath db --fork --logpath db/mongodb.log

- Run and install ``teleprom.pt`` server

  .. code-block:: sh

    git clone git@github.com:nleite/teleprom.pt.git
    npm build 
    npm link 
    /usr/local/bin/teleprom.pt --help
    /usr/local/bin/teleprom.pt --file subs/-6VlDZ9QimE.srt &

- Run ``speach_speed.py`` script

  .. code-block:: sh

    wget https://raw.githubusercontent.com/nleite/mdb.local/master/sparktalk/speach_speed.py
    spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/word.counter" \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 \
    speach_speed.py


