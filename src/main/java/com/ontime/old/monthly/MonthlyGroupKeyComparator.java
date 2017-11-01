package com.ontime.old.monthly;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MonthlyGroupKeyComparator extends WritableComparator {
	
	public MonthlyGroupKeyComparator() {
		super(MonthlyComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MonthlyComplexKey key1 = (MonthlyComplexKey) a;
		MonthlyComplexKey key2 = (MonthlyComplexKey) b;
		
		return key1.getOrigin().compareTo(key2.getOrigin());
	}
}