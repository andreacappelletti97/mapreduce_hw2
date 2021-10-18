#!/bin/bash

JAR="acappe2_hw2.jar"

#Clean up input_dir
hadoop fs -rm -rf input_dir
hadoop fs -mkdir input_dir

#Clean up
hadoop fs -rm -rf output_dir
rm -rf output_dir

#Run hadoop
hadoop jar $JAR input_dir output_dir

#Get the output
hadoop fs -get output_dir/