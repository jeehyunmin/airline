package com.ontime.monthly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.ontime.old.monthly.MonthlyComplexKey;
import com.ontime.old.monthly.MonthlyDelayReducer;
import com.ontime.origin.OriginDelayReducer;

public class MonthlyReducerTest {

	ReduceDriver<MonthlyComplexKey, IntWritable, MonthlyComplexKey, IntWritable> reduce;

	@Test
	public void test() throws IOException {
		reduce = ReduceDriver.newReduceDriver(new MonthlyDelayReducer());
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		List<IntWritable> values2 = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		reduce.withInput(new MonthlyComplexKey("D,SFO",1), values);
		reduce.withInput(new MonthlyComplexKey("A,SFO",5), values2);
		reduce.withOutput(new MonthlyComplexKey("D,SFO",1), new IntWritable(5));
		reduce.withOutput(new MonthlyComplexKey("A,SFO",5), new IntWritable(3));

		// MultipleOutput으로 쪼갠 거 반영해주기
		reduce.withMultiOutput("departure", new MonthlyComplexKey("SFO",1), new IntWritable(5));
		reduce.withMultiOutput("arriaval", new MonthlyComplexKey("SFO",5), new IntWritable(3));

	}

}
