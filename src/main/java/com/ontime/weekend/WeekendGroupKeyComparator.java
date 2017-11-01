package com.ontime.weekend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeekendGroupKeyComparator extends WritableComparator {
	
	public WeekendGroupKeyComparator() {
		super(WeekendComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		WeekendComplexKey key1 = (WeekendComplexKey) a;
		WeekendComplexKey key2 = (WeekendComplexKey) b;
		
		return key1.getOrigin().compareTo(key2.getOrigin());
	}
}