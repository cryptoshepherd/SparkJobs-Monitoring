# cloudera-spark-jobs-monitoring
A python monitoring solution for your Cloudera Spark jobs

A Python Docker Image to monitoring your Cloudera Spark jobs using K8s cronjob

This is my first python journey, in fact this project is not intended to be perfect, neither production ready. 
The aim was to implement and automate something capable to monitoring our Spark jobs running on Cludera.

The kubernetes cluster used for the project has not direct access to the internet. 
The Teams webhook calls are made via corporate proxy
