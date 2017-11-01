package com.ontime.carrier;

import java.util.Date;

import org.apache.hadoop.io.Text;

import com.ontime.common.AirlineParser2;

public class Test {
	
	public static void main(String[] args) {
		String record1 = "2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA";
		String record12 = "1999,12,19,7,1907,1910,2114,2117,AA,1359,N2CUAA,127,127,103,-3,-3,BNA,DFW,631,11,13,0,NA,0,NA,NA,NA,NA,NA";
		
		AirlineParser2 parser = new  AirlineParser2(record1);
		
		Date hour = parser.getDepTime();
		
		System.out.println("dataset record1 : " + record1);
		System.out.println(hour);
		
		
	}
}
