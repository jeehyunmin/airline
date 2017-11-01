package com.ontime.origin.month;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MonthComplexKeyComparator extends WritableComparator {
	
	public MonthComplexKeyComparator() {
		super(MonthComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MonthComplexKey key1 = (MonthComplexKey) a;
		MonthComplexKey key2 = (MonthComplexKey) b;
		
		//origin 비교
		int count = key1.getOrigin().compareTo(key2.getOrigin());
		if(count != 0) return count;
		
		//month 비교
		return key1.getMonth() == key2.getMonth() ? 0 : (key1.getMonth() < key2.getMonth() ? -1 :1 );
	}
}
