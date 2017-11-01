package com.ontime.origin.date;

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
public class DateComplexKey implements WritableComparable<DateComplexKey>{
	
//	origin, date

	private String origin;
	private Integer date;

	@Override
	public String toString() {
		return origin+"," +date;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readInt();
		origin = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(date);
		WritableUtils.writeString(out, origin);
	}

	@Override
	public int compareTo(DateComplexKey key) {
		int result = origin.compareTo(key.origin);
		
		if(0 ==result){
			result = date.compareTo(key.date);
		}
		return result;
	}
}
