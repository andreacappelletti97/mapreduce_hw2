#!/bin/bash

JAR="acappe2_hw2.jar"

#Clean up input_dir
hadoop fs -rm -r input_dir
hadoop fs -mkdir input_dir

hadoop fs -put input.log input_dir/
hadoop fs -ls input_dir/

#Clean up output dir
hadoop fs -rm -r output_dir
rm -r output_dir

#Run hadoop
hadoop jar $JAR input_dir output_dir

#Get the output
hadoop fs -get output_dir/