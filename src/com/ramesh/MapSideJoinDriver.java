package com.ramesh.MapperJoin;



import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;


public class MapSideJoinDriver extends Configured implements Tool {

	public static class KeyPartitioner implements Partitioner<TextPair, Text> {
    		@Override
    		public void configure(JobConf job) {}
    
    		@Override
    		public int getPartition(TextPair key, Text value, int numPartitions) {
      			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    		}
  	}
	
	@Override
	public int run(String[] args) throws Exception {
		args = new String[] { 
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/MapSideJoin/Input_data/DeptName.txt",
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/MapSideJoin/Input_data/DeptStrength.txt",
    			"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/MapSideJoin/Output_Data/"};
				 
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[2])); 
				 
				if (args.length != 3) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
				
				System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
				

		JobConf conf = new JobConf(getConf(), getClass());
    		conf.setJobName("Join 'Department Emp Strength input' with 'Department Name input'");

		Path AInputPath = new Path(args[0]);
		Path BInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(conf, AInputPath, TextInputFormat.class, DeptNameMapper.class);
		MultipleInputs.addInputPath(conf, BInputPath, TextInputFormat.class, DeptEmpStrengthMapper.class);

		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setPartitionerClass(KeyPartitioner.class);
    		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);
    
    		conf.setMapOutputKeyClass(TextPair.class);
    
   		conf.setReducerClass(MapSideJoinReducer.class);

    		conf.setOutputKeyClass(Text.class);
    
    		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new MapSideJoinDriver(), args);
		System.exit(exitCode);
	}
}