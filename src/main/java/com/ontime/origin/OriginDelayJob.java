package com.ontime.origin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OriginDelayJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		 	ToolRunner.run(new OriginDelayJob(), args);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
//		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
//		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(getConf(), "Origin Delay Counts");
		
		job.setJar("target/airline-0.0.1.jar");
		job.setJarByClass(OriginDelayJob.class);

//		Path inputDir = new Path("dataexpo/1987_nohead.csv");
//		Path outputDir = new Path("result/origin/1987");
		
		Path inputDir = new Path("dataexpo");
		Path outputDir = new Path("result/origin/");
		
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(OriginDelayMapper.class);
		job.setReducerClass(OriginDelayReducer.class);
//		job.setCombinerClass(OriginDelayReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "origin-departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "origin-arrival", TextOutputFormat.class, Text.class, IntWritable.class);

		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
