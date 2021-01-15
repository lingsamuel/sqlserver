#!/bin/bash

HASH=$(docker create $HBASE_TAG)
docker container cp $HASH:/hbase output/hbase
docker rm $HASH