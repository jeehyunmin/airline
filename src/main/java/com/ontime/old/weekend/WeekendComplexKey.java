package com.ontime.old.weekend;

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
public class WeekendComplexKey implements WritableComparable<WeekendComplexKey>{
	
//	origin, weekendly

	private String origin;
	private Integer weekend;

	@Override
	public String toString() {
//		return weekend +"," +origin;
		return origin+"," +weekend;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		weekend = in.readInt();
		origin = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(weekend);
		WritableUtils.writeString(out, origin);
	}

	@Override
	public int compareTo(WeekendComplexKey key) {
		int result = origin.compareTo(key.origin);
		
		if(0 ==result){
			result = weekend.compareTo(key.weekend);
		}
		
		return 0;
	}
}
