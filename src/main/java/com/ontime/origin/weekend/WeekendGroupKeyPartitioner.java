package com.ontime.origin.weekend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeekendGroupKeyPartitioner extends Partitioner<WeekendComplexKey, IntWritable>{

	@Override
	public int getPartition(WeekendComplexKey key, IntWritable value, int numPartitions) {
		
		int hash = key.getOrigin().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	}

}
