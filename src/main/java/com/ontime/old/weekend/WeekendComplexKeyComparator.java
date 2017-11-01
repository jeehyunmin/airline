package com.ontime.old.weekend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeekendComplexKeyComparator extends WritableComparator {
	
	public WeekendComplexKeyComparator() {
		super(WeekendComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		WeekendComplexKey key1 = (WeekendComplexKey) a;
		WeekendComplexKey key2 = (WeekendComplexKey) b;
		
		//origin 비교
		int count = key1.getOrigin().compareTo(key2.getOrigin());
		if(count != 0) return count;
		
		//month 비교
		return key1.getWeekend() == key2.getWeekend() ? 0 : (key1.getWeekend() < key2.getWeekend() ? -1 :1 );
	}
}
