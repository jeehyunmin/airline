package com.ontime.origin.month;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthGroupKeyPartitioner extends Partitioner<MonthComplexKey, IntWritable>{

	@Override
	public int getPartition(MonthComplexKey key, IntWritable value, int numPartitions) {
		
		int hash = key.getOrigin().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	}

}
