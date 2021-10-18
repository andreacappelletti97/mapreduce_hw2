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
In order to create the interval, the very first log message is taken into account and sent to every mapper through a configuration param.


Next step is to decide time window in milliseconds, that we can set from the config
```
timeWindow = 5000
```
The TimeInterval job will automatically generate our interval for us.
In fact, once the mapper knows the first time interval and the time window, it is easy
to compute the interval of a given time interval.
```
time_interval = (current_time - start_time)/ time_windows
```
### Input


### Output
Indeed the output will be a CSV with the time interval and the log message
```
0,20:13:54.458 [scala-execution-context-global-90] WARN  HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z
1,20:18:54.482 [scala-execution-context-global-90] INFO  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(
```

As we can see the first log message is in time interval 0 and the second one is in time interval 1.
The entire output will be stored into the output_dir/time_interval directory and 
then fed as input in the next jobs.

# Job0

> Compute a spreadsheet or an CSV file that shows the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types.


### Input
The input of this mapreduce job is taken from the output_dir/time_interval directory because we have to take into account different time intervals.

### Output
The output of this mapreduce job is a CSV file


# Job1
> Compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances.


### Sorting
In order to sort the output of this mapreduce I could have created a second mapreduce job with Secondary Sorting techniques.
I decided not to do it and flip the key and the values into the CSV final output, changing the standard comparator and sorting them in a descending order.
The reason behind my decision is not to run another time consuming mapreduce job, indeed the CSV column order does not affect the integrity of the data.

### Input
The input of this mapreduce job is taken from the output_dir/time_interval directory because we have to take into account different time intervals.

### Output
The output of this mapreduce job is a CSV file with the number of matches found and the time interval ordered in a descending order by number of matches found.

```
55, 3
12, 0
```
55 and 12 are the number of matches, 3 and 0 are the time intervals of the log messages.

# Job2
> For each message type you will produce the number of the generated log messages.
### Output

# Job3
> Produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

### Output

# AWS YouTube Video

# Run the mapreduce
The section below explains how to run the mapreduce jobs present into this project.
Before running the mapreduce, take into account the following assumptions.

For simplicity we consider just one entire log file, the name has to be specified into the config of our project in order to retrieve
the first line of the file and then create the time intervals.

The standard log file used to run the mapreduce so far is named input.log and is placed into the input_dir/ directory in HDFS.

## Installation
There are mainly two ways to build the jobs: through IntelliJ or via command line.

This section explains how to properly build them.

In order to build the project we have first to clone this repo
```
git clone https://github.com/andreacappelletti97/cloudsimplus_hw1.git
```

### Command Line
In order to build the jobs from cli we need to use sbt

Run the following commands from the root directory of the project

```
sbt clean compile test
```

The output should be the following


Create the jar of the project launching the following command
```
sbt clean compile assembly
```


Or alternatevely we can run the project locally with

```
sbt clean compile run
```
Please note that in order to run the mapreduce locally you should create an input_dir in the root directory of the project and put inside it the log file you want to parse with the jobs.

## Testing environment
In order to run the mapreduce jobs you can also setup a test environment with Hortons Sandbox.
In this section you will find detailed steps that will lead you there.

Disclamer, this configuration runs with the following versions
- macOS Big Sur (11.6)
- HDP_3.0.1
- VmWare Fusion

You will have to adapt this guide to your configuration accordling.

FIrst thing first, download VMWare, the virtual machine environment on which you will run the software stack of Hortons.
You can find it directly on the UIC web store: http://go.uic.edu/csvmware
After the download is completed, run the installation procedure.
Once completed, download the hortons sandbox image: https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html
Install the image just downloaded on VMWare and run the virtual machine.

Just to simplify a little bit things, you can add to your /etc/hosts the line below
```
172.16.67.2     sandbox-hdp.hortonworks.com
```
Where 172.16.67.2 is the ip address of your running sandbox.
In this way you can call ssh, scp and other useful command directly with the host address sandbox-hdp.hortonworks.com.

In order to run the entire mapreduce in the testing enviroment 
I wrote a bash script that will automatically run the commands for you and its located in the root of the project named deploy.sh.
I will break it down.
First thing first, we have our VMWare running with Hortons. Let's compile our jar. 
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
