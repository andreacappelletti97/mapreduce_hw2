# Homework 2
### The goal of this homework is for students to gain experience with solving a distributed computational problem using cloud computing technologies. The main textbook group (option 1) will design and implement an instance of the map/reduce computational model. 
### Grade: 9%

## Author
Andrea Cappelletti  
UIN: 674197701   
acappe2@uic.edu


# TimeInterval job
This job leads to the creation of different time intervals
to analyse the overall logs.

For simplicity, we consider just one entire log file, the name has to be specified into the config of our project in order to retrieve
the first line of the file and then create the time intervals.

The standard log file used to run the mapreduce so far is named <code>input.log</code> and is placed into the <code>input_dir/</code> directory in HDFS.

In order to create the interval, the very first log message is taken into account and sent to every mapper through a configuration param.
```
conf.set("startTime", startTimeInterval)
```
Next step is to decide time window in milliseconds, that we can set from the config
```
timeWindow = 5000
```
The TimeInterval job will automatically generate our interval for us.

In fact, once the mapper knows the first time interval and the time window, it is easy
to compute the interval of a given time interval.

![alt text](assets/equation.png)

The output is all the log messages with the defined time interval.

### Mapper
The mapper is into the <code>TimeMapper</code> class.

It basically takes the log lines one by one, detect the timestamp of the log,
computes the time interval and return the time interval as a key and the entire log message as a value.

The time interval is computed as explained above and then rounded.

We can consider the example of key-value pair below. Where 1024 is the time interval and the value is the log message.
```
key : 1024
value: 20:18:54.482 [scala-execution-context-global-90] INFO  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(
```
### Reducer
The reducer is into the <code>TimeReducer</code> class.
It takes each key from the mapper and simply output all the values computed.

### Input
The input of this mapreduce job is taken from the<code> input_dir/ </code>directory, that contains the initial log messages file.

### Output
The output will be a CSV with the time interval and the log message
```
0,20:13:54.458 [scala-execution-context-global-90] WARN  HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z
1,20:18:54.482 [scala-execution-context-global-90] INFO  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(
```

As we can see the first log message is in time interval 0 and the second one is in time interval 1.

The entire output will be stored into the <code>output_dir/time_interval</code> directory and 
then fed as input in the next jobs.

# Job0

> Compute a spreadsheet or an CSV file that shows the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types.

### Mapper
The mapper is into the <code>Job0Mapper</code> class.

It basically takes each log message as input with the time interval computed in the previous job.
It checks if the log message contains the regural expression we are looking for and print out the result.

In order to do that, we set the time interval and the log level (INFO,WARN, DEBUG ... ) as a key, the number 1 to compute the frequency sum and either 1 or 0 if the regural expression
is found into the message.

```
key : 1024, INFO
value: 1,1
```

In the example above we have a log message at time interval 1024 that contains the regular expression.

```
key : 1024, INFO
value: 1,0
```
In this second example, the log message does not contain the regex.

### Reducer
The reducer performs a <code>FoldLeft</code> operation to sum both the frequency of the log type in the interval and the
regex pattern matches.

### Input
The input of this mapreduce job is taken from the<code> output_dir/time_interval </code>directory because we have to take into account different time intervals.

### Output
The output of this mapreduce job is a CSV file written into the directory <code> output_dir/job0 </code>.
```
0,DEBUG,7,1
0,ERROR,3,1
0,INFO,66,4
0,WARN,28,0
1440,ERROR,2,2
```
The first column represents the time interval, the second the log type, the third
the log type frequency into that interval and finally the number of matches.


# Job1
> Compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances.


### Mapper
The mapper is into the <code>Job1Mapper</code> class.

It basically takes the input log message line and creates the time interval combined with the log type as a key.
The log message type in this case is just ERROR, because we are looking for ERRROs only.
The value is either 1 or 0 if the log message contains matches of the regular expression or not.

```
key : 1024, ERROR
value: 1
```

### Reducer
The reducer takes as input the time intervals with the log type as key and sum all the values found with
a FoldLeft function.

### Input
The input of this mapreduce job is taken from the output_dir/time_interval directory because we have to take into account different time intervals.

### Output
The output of this mapreduce job is a CSV file with the number of matches found and the time interval ordered in descending order by number of matches found.
```
2,1440
1,0
```
2 and 1 are the number of matches, 1440 and 0 are the time intervals of the log messages.

### Sorting
In order to sort the output of this mapreduce I could have created a second mapreduce job with Secondary Sorting techniques.

I decided not to do it and flip the key and the values into the CSV final output, changing the standard comparator in the driver class <code>Job1Driver</code> and sorting them in descending order.
```
job.setSortComparatorClass(classOf[LongWritable.DecreasingComparator])
```
The reason behind my decision is not to run another time consuming mapreduce job, indeed the CSV column order does not affect the integrity of the data.

# Job2
> For each message type you will produce the number of the generated log messages.
### Mapper
### Reducer
### Input
### Output

# Job3
> Produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

### Mapper
### Reducer
### Input
### Output

# AWS YouTube Video

# Run the mapreduce
The section below explains how to run the mapreduce jobs present into this project.
Before running the mapreduce, take into account the following assumptions.

For simplicity, we consider just one entire log file, the name has to be specified into the config of our project in order to retrieve
the first line of the file and then create the time intervals.

The standard log file used to run the mapreduce so far is named <code>input.log</code> and is placed into the <code>input_dir/</code> directory in HDFS.

## Installation
Please note that in order to properly build the jar, you have to setup
the right system and version configuration in the<code> build.sbt </code> and <code>project/plugins.sbt </code> files.

Both the instances are provided into this repository. 

It may happen that you have to change them according to your system setup and java version. In order to do that
refer to the official documentation.

There are mainly two ways to build the jobs: through IntelliJ or via command line.

This section explains how to properly build them.

In order to build the project we have first to clone this repo
```
git clone https://github.com/andreacappelletti97/mapreduce_hw2.git
```

### Command Line
In order to build the jobs from cli we need to use sbt

Run the following commands from the root directory of the project

```
sbt clean compile test
```

The output should be the following


Create the jar if you want to run it on AWS or Hortons Sandbox.
Launch the following command
```
sbt clean compile assembly
```
The jar can be found under <code> target/scala-3.0.2/acappe2_hw2.jar </code> 

You can follow the next section to run it into the testing VMware Vm or the YouTube video to run it on Amazon AWS.

Or alternatevely we can run the project locally with

```
sbt clean compile run
```
Please note that in order to run the mapreduce locally you should create an <code> input_dir </code> in the root directory of the project and put inside it the log file you want to parse with the jobs.

The log file should be named <code>input.log </code>, or according to the configuration of your project.

## Testing environment
In order to run the mapreduce jobs you can also setup a test environment with Hortons Sandbox.

In this section you will find detailed steps that will lead you there.

Disclaimer, this configuration runs with the following versions
- macOS Big Sur (11.6)
- HDP_3.0.1
- VmWare Fusion

You will have to adapt this guide to your configuration accordling.

FIrst thing first, download VMWare, the virtual machine environment on which you will run the software stack of Hortons.
You can find it directly on the UIC web store: http://go.uic.edu/csvmware

After the download is completed, run the installation procedure.  

Once completed, download the hortons sandbox image: https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html

Install the image just downloaded on VMWare and run the virtual machine.

Just to simplify a little things, you can add to your <code>/etc/hosts</code> the line below
```
172.16.67.2     sandbox-hdp.hortonworks.com
```
Where <code>172.16.67.2</code> is the ip address of your running sandbox.
In this way you can call ssh, scp and other useful command directly with the host address<code> sandbox-hdp.hortonworks.com </code>.

In order to run the entire mapreduce in the testing environment 
I wrote a bash script that will automatically run the commands for you and its located in the root of the project named <code>deploy.sh </code>.

```shell
#!/bin/bash

sbt clean compile assembly

cd target/scala-3.0.2/

scp -P 2222 acappe2_hw2.jar root@sandbox-hdp.hortonworks.com:~/

ssh root@sandbox-hdp.hortonworks.com -p 2222
```
I will break it down.

First thing first, we have our VMWare running with Hortons.  
Let's compile our jar. 

A detailed guide on how to do so is already explained in the section above.

In order to do that, we launch this command from the root of our project
```
sbt clean compile assembly
```
Then, we go to the directory of our jar

```
cd target/scala-3.0.2/
```

Then we copy the jar on the Hortons VM with scp

```
scp -P 2222 acappe2_hw2.jar root@sandbox-hdp.hortonworks.com:~/

```
Log into the Virtual Machine

```
ssh root@sandbox-hdp.hortonworks.com -p 2222
```

## Programming technology
All the simulations has been written in Scala using a Functional Programming approach.

While writing the simulations the following best practices has been adopted

- Large usage of logging to understand the system status;


- Configuration libraries and files to provide input values for the simulations;


- No while or for loop is present, instead recursive functions are largely used into the project.


- Modular architecture (code is divided by module to enhance mainteinability)



## References
In order to produce the result obtained the following documents and paper
have been consulted.

- https://leanpub.com/edocc
- http://hadoop.apache.org
- https://www.cloudera.com/downloads/hortonworks-sandbox.html
- https://www.vmware.com
