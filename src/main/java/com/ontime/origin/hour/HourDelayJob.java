package com.ontime.origin.hour;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HourDelayJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		 	ToolRunner.run(new HourDelayJob(), args);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
//		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
//		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(conf, "Hour Delay Counts");
		
		job.setJar("target/airline-0.0.1.jar");
		job.setJarByClass(HourDelayJob.class);
		
		job.setPartitionerClass(HourGroupKeyPartitioner.class);
		job.setGroupingComparatorClass(HourComplexKeyComparator.class);
		
//		Path inputDir = new Path("dataexpo");
//		Path outputDir = new Path("result/hour/");
		
		Path inputDir = new Path("dataexpo/1987_nohead.csv");
		Path outputDir = new Path("result/hour/1987");
		
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(HourDelayMapper.class);
		job.setReducerClass(HourDelayReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(HourComplexKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(HourComplexKey.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
