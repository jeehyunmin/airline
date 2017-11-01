package com.ontime.monthly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class MonthlyDelayJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		 	ToolRunner.run(new MonthlyDelayJob(), args);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
//		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
//		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(conf, "Monthly Delay Counts");
		
		job.setJar("target/airline-0.0.1.jar");
		job.setJarByClass(MonthlyDelayJob.class);
		
//		job.setPartitionerClass(MonthlyGroupKeyPartitioner.class);
//		job.setGroupingComparatorClass(MonthlyGroupKeyComparator.class);
//		job.setSortComparatorClass(MonthlyComplexKeyComparator.class);

//		Path inputDir = new Path("dataexpo/1987_nohead.csv");
//		Path outputDir = new Path("result/monthly/1987");
		
		Path inputDir = new Path("dataexpo");
		Path outputDir = new Path("result/monthly/");
		
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(MonthlyDelayMapper.class);
		job.setReducerClass(MonthlyDelayReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(MonthlyComplexKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(MonthlyComplexKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "month-departure", TextOutputFormat.class, MonthlyComplexKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "month-arrival", TextOutputFormat.class, MonthlyComplexKey.class, IntWritable.class);

		
		job.waitForCompletion(true);
		
		return 0;
	}

}
