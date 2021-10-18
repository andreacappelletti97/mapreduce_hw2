#!/bin/bash

scp -P 2222 input_dir/input.log root@sandbox-hdp.hortonworks.com:~/

sbt clean compile assembly

cd target/scala-3.0.2/

scp -P 2222 acappe2_hw2.jar root@sandbox-hdp.hortonworks.com:~/

ssh root@sandbox-hdp.hortonworks.com -p 2222