package com.ontime.common;

import java.util.Date;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.ontime.common.AirlineParser;

import lombok.extern.java.Log;

@Log
public class AirlineParserTest {
	
	String record1 = "2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA";
	String record12 = "1999,12,19,7,1907,1910,2114,2117,AA,1359,N2CUAA,127,127,103,-3,-3,BNA,DFW,631,11,13,0,NA,0,NA,NA,NA,NA,NA";
	
	
	@Test
	public void test() {
		/*
		 * dataset 호출 정상 작동 여부
		 */
		Text text = new Text(record1);
		AirlineParser parser = new  AirlineParser(text);
		
		System.out.println(record1);
	}
	
	@Test
	public void testparser1() {
		/*
		 * dataset 호출 정상 작동 여부
		 */
		Text text = new Text(record1);
		AirlineParser parser = new  AirlineParser(text);
		
		Integer year = parser.getYear();
		Integer dayofWeek = parser.getDayofWeek();
		Integer depDelay = parser.getDepDelay();
		Integer arrDelay = parser.getArrDelay();
		
		System.out.println("dataset record1 : " + record1);
		System.out.println(year + "," + dayofWeek +"," + depDelay + "," + arrDelay);
		
	}
	
	@Test
	public void testparseTime() {
		/*
		 * dataset 호출 정상 작동 여부
		 */
		Text text = new Text(record1);
		AirlineParser parser = new  AirlineParser(text);
		
		Integer hour = parser.getDepTime();
		System.out.println("dataset record1 : " + record1);
		System.out.println("hour : " + hour);
		
	}
	
	
//	
//	@Test
//	public void testparser() throws Exception {
//		Text textParser = new Text(record12);
//		AirlineParser parserParser = new  AirlineParser(textParser);
//		
//		Integer year = parserParser.getYear();
//		Integer dayofWeek = parserParser.getDayofWeek();
//		Integer depDelay = parserParser.getDepDelay();
//		Integer arrDelay = parserParser.getArrDelay();
//		
//		System.out.println("dataset record2 : " + record12);
//		System.out.println(year + "," + dayofWeek +"," + depDelay + "," + arrDelay);
//	}

}
