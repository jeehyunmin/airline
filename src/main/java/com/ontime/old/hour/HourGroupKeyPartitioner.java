package com.ontime.old.hour;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HourGroupKeyPartitioner extends Partitioner<HourComplexKey, IntWritable>{

	@Override
	public int getPartition(HourComplexKey key, IntWritable value, int numPartitions) {
		
		int hash = key.getOrigin().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	}

}
