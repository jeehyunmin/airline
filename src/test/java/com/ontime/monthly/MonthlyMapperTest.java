package com.ontime.monthly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.ontime.old.monthly.MonthlyComplexKey;
import com.ontime.old.monthly.MonthlyDelayMapper;
import com.ontime.origin.OriginDelayMapper;


public class MonthlyMapperTest {

	MapDriver<LongWritable, Text, MonthlyComplexKey, IntWritable> map;
	
	String record1 = "2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA";
	String record19 = "1999,12,19,7,1907,1910,2114,2117,AA,1359,N2CUAA,127,127,103,-3,-3,BNA,DFW,631,11,13,0,NA,0,NA,NA,NA,NA,NA";
	
	@Test
	public void testRecord1() throws IOException {
		
//		2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA
		map = MapDriver.newMapDriver(new MonthlyDelayMapper());

		map.withInput(new LongWritable(0), new Text(record1));
		map.withOutput(new MonthlyComplexKey("D, IAD", 1), new IntWritable(8));
		System.out.println("builds test");
//		map.runTest();
	}

}
