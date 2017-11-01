package com.ontime.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.java.Log;

@Log
@Getter
@Setter
@ToString
public class AirlineParser2 {

	private Integer year;
	private Integer month;
	private Integer dayofMonth;	
	private Integer dayofWeek;				//1(Monday) - 7 (Sunday)
	private Date depTime;
	private Date cRSDepTime;
	private Date arrTime;
	private Date cRSArrTime;
	private String uniqueCarrier;
	private Integer flightNum;
	private String tailNum;
	private Integer actureElapsedTime;
	private Integer cRSElapsedTime;
	private Integer airTime;
	private Integer arrDelay;
	private Integer depDelay;
	private String origin;
	private String dest;
	private Integer distance;
	private Integer taxiIn;
	private Integer taxiOut;
	private Integer cancelled;
	private String cancellationCode;
	private Integer diverted;								//1 = yes, 0 = no
	private Integer carrierDelay;
	private Integer weatherDelay;
	private Integer nasDelay;
	private Integer securityDelay;
	private Integer lateAircraftDelay;
	
	SimpleDateFormat dateformat = new SimpleDateFormat("mm");
	
	/* 
	 * 필수값
		Year, Month, DayofMonth, DayOfWeek, CRSDepTime, CRSArrTime, UniqueCarrier, FlightNum,
		Origin, Dest, Cancelled, Diverted
	 */
	
	private boolean depTimeAvailable = true;
	private boolean arrTimeAvailable = true;
	private boolean tailNumAvailable = true;
	private boolean actualElapsedTimeAvailable = true;
	private boolean cRSElapsedTimeAvailable = true;
	private boolean airTimeAvailable = true;
	private boolean arrDelayAvailable = true;
	private boolean depDelayAvailable = true;
	private boolean distanceAvailable = true;
	private boolean taxiInAvailable = true;
	private boolean taxiOutAvailable = true;
	private boolean cancellationCodeAvailable = true;
	private boolean carrierDelayAvailable = true;
	private boolean weatherDelayAvailable = true;
	private boolean nasDelayAvailable = true;
	private boolean securityDelayAvailable = true;
	private boolean lateAircraftDelayAvailable = true;

	
	public AirlineParser2(String text) {
		
		String[] columns = text.toString().split(",");
		
		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);
		dayofMonth = Integer.parseInt(columns[2]);
		dayofWeek = Integer.parseInt(columns[3]);
		
		try {
			
			if(!columns[4].equals("NA")){
				depTime = dateformat.parse(columns[4]);
			} else{
				depTimeAvailable = false;
			}
			cRSDepTime = dateformat.parse(columns[5]);
			if(!columns[6].equals("NA")){
				arrTime = dateformat.parse(columns[6]);
			} else{
				arrTimeAvailable = false;
			}
			cRSArrTime = dateformat.parse(columns[7]);
			
		} catch (ParseException e) {
			e.printStackTrace();
			log.info("date format parser error : " + e.getMessage());
		}

		uniqueCarrier = columns[8];
		flightNum = Integer.parseInt(columns[9]);

		if(!columns[10].equals("NA")){
			tailNum = columns[10];
		} else{
			tailNumAvailable = false;
		}

		if(!columns[11].equals("NA")){
			actureElapsedTime = Integer.parseInt(columns[11]);
		} else{
			actualElapsedTimeAvailable = false;
		}
		
		if(!columns[12].equals("NA")){
			cRSElapsedTime = Integer.parseInt(columns[12]);
		} else {
			cRSElapsedTimeAvailable = false;
		}
		if(!columns[13].equals("NA")){
			airTime = Integer.parseInt(columns[13]);
		} else {
			airTimeAvailable = false;
		}
		if(!columns[14].equals("NA")){
			arrDelay = Integer.parseInt(columns[14]);
		} else {
			arrDelayAvailable = false;
		}
		if(!columns[15].equals("NA")){
			depDelay = Integer.parseInt(columns[15]);
		} else {
			depDelayAvailable = false;
		}
		
		origin = columns[16];
		dest = columns[17];
		
		if(!columns[18].equals("NA")){
			distance = Integer.parseInt(columns[18]);
		} else {
			distanceAvailable = false;
		}
		if(!columns[19].equals("NA")){
			taxiIn = Integer.parseInt(columns[19]);
		} else {
			taxiInAvailable = false;
		}
		if(!columns[20].equals("NA")){
			taxiOut = Integer.parseInt(columns[20]);
		} else {
			taxiOutAvailable = false;
		}
		
		cancelled = Integer.parseInt(columns[21]);
		
		if(!columns[22].equals("NA")){
			cancellationCode = columns[22];
		} else {
			cancellationCodeAvailable = false;
		}
		
		diverted = Integer.parseInt(columns[23]);
		
		if(!columns[24].equals("NA")){
			carrierDelay = Integer.parseInt(columns[24]);
		} else {
			carrierDelayAvailable = false;
		}
	
		if(!columns[25].equals("NA")){
			weatherDelay = Integer.parseInt(columns[25]);
		} else {
			weatherDelayAvailable = false;
		}
		if(!columns[26].equals("NA")){
			nasDelay = Integer.parseInt(columns[26]);
		} else {
			nasDelayAvailable = false;
		}
		if(!columns[27].equals("NA")){
			securityDelay = Integer.parseInt(columns[27]);
		} else {
			securityDelayAvailable = false;
		}
		if(!columns[28].equals("NA")){
			lateAircraftDelay = Integer.parseInt(columns[28]);
		} else {
			lateAircraftDelayAvailable = false;
		}
	}
//	
//	public String minutes(String time){
//		String wholeMin;
//		char[] timeArray = time.toCharArray();
//		String hour ="";
//		
//		return wholeMin;
//	}
//	
}
