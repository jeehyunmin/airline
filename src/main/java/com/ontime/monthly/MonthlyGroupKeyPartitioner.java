package com.ontime.monthly;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthlyGroupKeyPartitioner extends Partitioner<MonthlyComplexKey, IntWritable>{

	@Override
	public int getPartition(MonthlyComplexKey key, IntWritable value, int numPartitions) {
		
		int hash = key.getOrigin().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	}

}
