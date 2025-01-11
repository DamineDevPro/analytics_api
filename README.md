### py-spark-kafka-setup

<h1>Django py-spark analytics</h1>

![Python](https://img.shields.io/badge/python-v3.6.9+-blue.svg)
![Spark](https://img.shields.io/badge/Spark-2.4.5-red.svg)
![Django](https://img.shields.io/badge/Django-3.0.0+-red.svg)

---------------------------------------
<h2>Installation on Ubuntu System</h2>

1.  sudo apt-get update
2.  pip install -U pip
3.  pip install -U setuptools
-----------------------------------------------------
<b> Cassandra Spark Connector</b>

4.  git clone -b b2.5 https://github.com/datastax/spark-cassandra-connector.git
5.  ./sbt/sbt test
6.  ./sbt/sbt it:test
7.  ./sbt/sbt test:package
8.  COPY com.github.jnr_jffi-1.2.19.jar /.ivy2/jars
9.  COPY org.codehaus.groovy_groovy-2.5.7.jar /.ivy2/jars
10. COPY org.codehaus.groovy_groovy-json-2.5.7.jar /.ivy2/jars
----------------------------------------------------
<b>Mongo Spark Connector</b> 

11. git clone https://github.com/mongodb/mongo-spark.git
12. cd mongo-spark
13. ./sbt check
----------------------------------------------------
<b>Install the dependency and running the project</b>

14. pip3 install -r requirements.txt
15. run ecosystem.config.js





