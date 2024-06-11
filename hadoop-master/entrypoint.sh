#!/bin/bash

if [ ! -d "/usr/local/hadoop/data/namenode" ]; then
  $HADOOP_HOME/bin/hdfs namenode -format
fi

$HADOOP_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
$HADOOP_HOME/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

$HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
$HADOOP_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager

tail -f $HADOOP_HOME/logs/*
