package com.ontime.hour;

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
public class HourComplexKey implements WritableComparable<HourComplexKey>{
	
//	origin, hourly

	private String origin;
	private Integer hour;

	@Override
	public String toString() {
//		return hour +"," +origin;
		return origin+"," +hour;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		hour = in.readInt();
		origin = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hour);
		WritableUtils.writeString(out, origin);
	}

	@Override
	public int compareTo(HourComplexKey key) {
		int result = origin.compareTo(key.origin);
		
		if(0 ==result){
			result = hour.compareTo(key.hour);
		}
		
		return 0;
	}
}
