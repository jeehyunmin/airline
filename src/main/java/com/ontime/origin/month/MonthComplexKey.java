package com.ontime.origin.month;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MonthComplexKey implements WritableComparable<MonthComplexKey>{
	
//	origin, month

	private String origin;
	private Integer month;

	@Override
	public String toString() {
		return origin+"," +month;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		month = in.readInt();
		origin = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(month);
		WritableUtils.writeString(out, origin);
	}

	@Override
	public int compareTo(MonthComplexKey key) {
		int result = origin.compareTo(key.origin);
		
		if(0 ==result){
			result = month.compareTo(key.month);
		}
		return result;
	}
}
