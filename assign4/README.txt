										*********ASSIGNMENT – 4*********
											      READ ME


++-TERMS-++

--> Mapper - Mapper maps input key/value pairs to a set of intermediate key/value pairs.Maps are the individual tasks that transform input records into intermediate records.
--> Reducer - Reducer reduces a set of intermediate values which share a key to a smaller set of values.The number of reduces for the job is set by the user via Job.setNumReduceTasks(int).
	a) Shuffle - Input to the Reducer is the sorted output of the mappers. In this phase the framework fetches the relevant partition of the output of all the mappers, via HTTP.
	b) Sort - The framework groups Reducer inputs by keys (since different mappers may have output the same key) in this stage.The shuffle and sort phases occur simultaneously; while map-outputs are 			  being fetched they are merged.
	c) Reduce -In this phase the reduce(WritableComparable, Iterable<Writable>, Context) method is called for each <key, (list of values)> pair in the grouped inputs.The output of the reduce task is 			   typically written to the FileSystem via Context.write(WritableComparable, Writable).Applications can use the Counter to report its statistics.The output of the Reducer is not sorted. 

++-MY PROGRAM-++

1) Placed the input file in the HDFS file system.
2) The main function create a configuration and uses it to create a new job - mapperreducer job, the class that is used for this purpose is "Equijoin".
3) The function initFunc is used for the purpose of setting up of parameters to the job that was created and specifying to the job various mapper, reducer class etc.
4) The path of the inputfile and outputfile is passed as an argument to the main.
5) We create a new mapper class for the purpose of join which extends the inbuilt class Mapper.
6) MapperClass -
		a) We replace all the spaces, tabs and newline present in a record in the input file.
		b) We extract the key from that record.
		c) We write the key and value .i.e the record using context.write(), thus we have created a map of key value pairs <key, value> from the input.
7) We create a new reducer class for the purpose of join which extends the inbuilt class Reducer.
8) ReducerClass - 
		a) We intitally create a new stringbuilder for the purpose of join.
		b) Then we combine each and every record from the mapped step into stringbuilder using a separtor ":", thus we have got all the tuples.
		c) We remove the additional separator ":" added to the end of the string builder.
		d) Then we split the records and store it into a string array inorder for the purpose of processing them with ease.
		e) Then we run a double loop inorder to check for every tuple with every other tuple.
		d) We check for the [0] value inside the loop as we have already mapped and kept the key value of the records who match-together, and we combine them together and write it to the output.
		f) We run the loop from 0 to array.length -1 and i+1 to array.length inorder to avoid duplicates in the output.
9) End of the program.

++-HOW TO RUN-++

1) Formatting the tmp node just to make sure to avoid any errors.
 		sudo rm -R /tmp/*
  		sudo rm -r /app/hadoop/tmp
  		sudo mkdir -p /app/hadoop/tmp
  		sudo chown hduser:hadoop /app/hadoop/tmp
  		sudo chmod 750 /app/hadoop/tmp

2) Formatting the data and name node to avoid errors.
  		/usr/local/hadoop/bin/hadoop namenode -format
  		/usr/local/hadoop/bin/hadoop datanode -format

3) Starting the HADOOP file system.
  		/usr/local/hadoop/sbin/start-all.sh

4) Making the input directory in HDFS and copying the input to the location.
  		bin/hdfs dfs -mkdir -p /hduser/data/
  		bin/hdfs dfs -put /home/hduser/Desktop/input.txt /hduser/data/

5) Running the join.
  		sudo -u hduser /usr/local/hadoop/bin/hadoop jar /home/hduser/workspace/cse512/target/cse512-1.0.jar Equijoin/cse512/Equijoin /hduser/data/input.txt /hduser/output

NOTE: if "output" folder already exsists remove it before running.

++-REFERENCE-++

- https://janzhou.org/2014/how-to-compile-hadoop.html





