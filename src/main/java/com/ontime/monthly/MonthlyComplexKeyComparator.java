package com.ontime.monthly;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MonthlyComplexKeyComparator extends WritableComparator {
	
	public MonthlyComplexKeyComparator() {
		super(MonthlyComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MonthlyComplexKey key1 = (MonthlyComplexKey) a;
		MonthlyComplexKey key2 = (MonthlyComplexKey) b;
		
		//origin 비교
		int count = key1.getOrigin().compareTo(key2.getOrigin());
		if(count != 0) return count;
		
		//month 비교
		return key1.getMonth() == key2.getMonth() ? 0 : (key1.getMonth() < key2.getMonth() ? -1 :1 );
	}
}
