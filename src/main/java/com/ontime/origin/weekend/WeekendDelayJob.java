package com.ontime.origin.weekend;

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

import com.ontime.origin.weekend.WeekendGroupKeyPartitioner;

public class WeekendDelayJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		 	ToolRunner.run(new WeekendDelayJob(), args);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
//		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
//		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(conf, "weekend Delay Counts");
		
		job.setJar("target/airline-0.0.1.jar");
		job.setJarByClass(WeekendDelayJob.class);
		
		job.setPartitionerClass(WeekendGroupKeyPartitioner.class);
		job.setGroupingComparatorClass(WeekendComplexKeyComparator.class);
//		job.setSortComparatorClass(WeekendComplexKeyComparator.class);
		
		
		Path inputDir = new Path("dataexpo");
		Path outputDir = new Path("result/weekend/");
		
//		Path inputDir = new Path("dataexpo/1987_nohead.csv");
//		Path outputDir = new Path("result/weekend/1987");
		
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(WeekendDelayMapper.class);
		job.setReducerClass(WeekendDelayReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(WeekendComplexKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(WeekendComplexKey.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
