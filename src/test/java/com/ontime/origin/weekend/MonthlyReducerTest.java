package com.ontime.origin.weekend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.ontime.origin.OriginDelayReducer;

public class MonthlyReducerTest {

	ReduceDriver<WeekendComplexKey, Text, WeekendComplexKey, Text> reduce;
	
	String record1 = "2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA";
	String record19 = "1999,12,19,7,1907,1910,2114,2117,AA,1359,N2CUAA,127,127,103,-3,-3,BNA,DFW,631,11,13,0,NA,0,NA,NA,NA,NA,NA";

	@Test
	public void test() throws IOException {
		reduce = ReduceDriver.newReduceDriver(new WeekendDelayReducer());
		
//		reduce.withInput(new WeekendComplexKey("D,SFO",1), new);
//		reduce.withInput(new WeekendComplexKey("A,SFO",5), values2);
//		reduce.withOutput(new WeekendComplexKey("D,SFO",1), new IntWritable(5));
//		reduce.withOutput(new WeekendComplexKey("A,SFO",5), new IntWritable(3));
//
//		// MultipleOutput으로 쪼갠 거 반영해주기
//		reduce.withMultiOutput("departure", new WeekendComplexKey("SFO",1), new IntWritable(5));
//		reduce.withMultiOutput("arriaval", new WeekendComplexKey("SFO",5), new IntWritable(3));

	}

}
