package com.ontime.origin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.ontime.origin.OriginDelayReducer;

public class OriginReducerTest {

	ReduceDriver<Text, IntWritable, Text, IntWritable> reduce;

	@Test
	public void test() throws IOException {
		reduce = ReduceDriver.newReduceDriver(new OriginDelayReducer());
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		reduce.withInput(new Text("D,SFO"), values);
		reduce.withOutput(new Text("D,SFO"), new IntWritable(5));

		// MultipleOutput으로 쪼갠 거 반영해주기
		reduce.withMultiOutput("departure", new Text("SFO"), new IntWritable(5));

	}

}
