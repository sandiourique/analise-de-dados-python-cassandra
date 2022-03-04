#!/bin/bash
echo "Criando"
gcloud dataproc clusters create mineracao --region=us-central1 --single-node \
    --image-version 2.0-debian10 --project fluid-vector-335716

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|                                                                           |"
echo "|                                                                           |"
echo "|                                                                           |"
echo "|                                                                           |"
echo "|                                                                           |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  JOB 1                                           |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc jobs submit pyspark --cluster mineracao --region=us-central1 \
	--py-files gs://5_scripts_diego/modules.zip \
    gs://5_scripts_diego/job_1.py

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|   +-------+                                                               |"
echo "|   |       |                                                               |"
echo "|   | JOB 1 |                                                               |"
echo "|   |       |                                                               |"
echo "|   +-------+                                                               |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  JOB 2                                           |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc jobs submit pyspark --cluster mineracao --region=us-central1 \
	--py-files gs://5_scripts_diego/modules.zip \
    gs://5_scripts_diego/job_2.py \
	--jars=gs://5_scripts_diego/postgresql-42.3.1.jar

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|   +-------+      +-------+                                                |"
echo "|   |       |      |       |                                                |"
echo "|   | JOB 1 |  >>  | JOB 2 |                                                |"
echo "|   |       |      |       |                                                |"
echo "|   +-------+      +-------+                                                |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  JOB 3                                           |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc jobs submit pyspark --cluster mineracao --region=us-central1 \
	--py-files gs://5_scripts_diego/modules.zip \
    gs://5_scripts_diego/job_3.py \
	--jars=gs://5_scripts_diego/postgresql-42.3.1.jar

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|   +-------+      +-------+      +-------+                                 |"
echo "|   |       |      |       |      |       |                                 |"
echo "|   | JOB 1 |  >>  | JOB 2 |  >>  | JOB 3 |                                 |"
echo "|   |       |      |       |      |       |                                 |"
echo "|   +-------+      +-------+      +-------+                                 |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  JOB 4                                           |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc jobs submit pyspark --cluster mineracao --region=us-central1 \
	--py-files gs://5_scripts_diego/modules.zip \
    gs://5_scripts_diego/job_4.py \
    --properties 'spark.jars.packages=com.datastax.spark:spark-cassandra-connector_2.12:3.1.0'

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|   +-------+      +-------+      +-------+      +-------+                  |"
echo "|   |       |      |       |      |       |      |       |                  |"
echo "|   | JOB 1 |  >>  | JOB 2 |  >>  | JOB 3 |  >>  | JOB 4 |                  |"
echo "|   |       |      |       |      |       |      |       |                  |"
echo "|   +-------+      +-------+      +-------+      +-------+                  |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  JOB 5                                           |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc jobs submit pyspark --cluster mineracao --region=us-central1 \
	--py-files gs://5_scripts_diego/modules.zip \
    gs://5_scripts_diego/job_5.py \
    --properties 'spark.jars.packages=com.datastax.spark:spark-cassandra-connector_2.12:3.1.0'

echo ""
echo "+---CLUSTER MINERCAO--------------------------------------------------------+"
echo "|                                                                           |"
echo "|   +-------+      +-------+      +-------+      +-------+      +-------+   |"
echo "|   |       |      |       |      |       |      |       |      |       |   |"
echo "|   | JOB 1 |  >>  | JOB 2 |  >>  | JOB 3 |  >>  | JOB 4 |  >>  | JOB 5 |   |"
echo "|   |       |      |       |      |       |      |       |      |       |   |"
echo "|   +-------+      +-------+      +-------+      +-------+      +-------+   |"
echo "|                                                                           |"
echo "+---------------------------------------------------------------------------+"
echo ""

echo ""
echo "+--------------------------------------------------+"
echo "|                                                  |"
echo "|  Excluindo cluster MINERACAO                     |"
echo "|                                                  |"
echo "+--------------------------------------------------+"
echo ""

gcloud dataproc clusters delete mineracao --region=us-central1 --quiet