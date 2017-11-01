package com.ontime.origin.date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateGroupKeyPartitioner extends Partitioner<DateComplexKey, IntWritable>{

	@Override
	public int getPartition(DateComplexKey key, IntWritable value, int numPartitions) {
		
		int hash = key.getOrigin().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	}

}
