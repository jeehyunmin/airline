package com.ontime.hour;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HourGroupKeyComparator extends WritableComparator {
	
	public HourGroupKeyComparator() {
		super(HourComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		HourComplexKey key1 = (HourComplexKey) a;
		HourComplexKey key2 = (HourComplexKey) b;
		
		return key1.getOrigin().compareTo(key2.getOrigin());
	}
}