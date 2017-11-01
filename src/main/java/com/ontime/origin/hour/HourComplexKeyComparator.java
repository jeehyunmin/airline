package com.ontime.origin.hour;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HourComplexKeyComparator extends WritableComparator {
	
	public HourComplexKeyComparator() {
		super(HourComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		HourComplexKey key1 = (HourComplexKey) a;
		HourComplexKey key2 = (HourComplexKey) b;
		
		//origin 비교
		int count = key1.getOrigin().compareTo(key2.getOrigin());
		if(count != 0) return count;
		
		//month 비교
		return key1.getHour() == key2.getHour() ? 0 : (key1.getHour() < key2.getHour() ? -1 :1 );
	}
}
