package com.ontime.origin.date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateComplexKeyComparator extends WritableComparator {
	
	public DateComplexKeyComparator() {
		super(DateComplexKey.class, true);
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateComplexKey key1 = (DateComplexKey) a;
		DateComplexKey key2 = (DateComplexKey) b;
		
		//origin 비교
		int count = key1.getOrigin().compareTo(key2.getOrigin());
		if(count != 0) return count;
		
		//month 비교
		return key1.getDate() == key2.getDate() ? 0 : (key1.getDate() < key2.getDate() ? -1 :1 );
	}
}
