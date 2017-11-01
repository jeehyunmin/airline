package com.ontime.origin.date;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.ontime.common.DelayCounters;
import com.ontime.common.AirlineParser;

import lombok.extern.java.Log;

@Log
public class DateDelayMapper extends Mapper<LongWritable, Text, DateComplexKey, Text> {
	
	private DateComplexKey outputKey = new DateComplexKey();
	private IntWritable valueCount = new IntWritable(1);
	private IntWritable valueTime = new IntWritable();
	private Text outputValue = new Text();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("monthly mapper started");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlineParser parser = new AirlineParser(value);

		// 출발 지연인 경우
		if(parser.isDepDelayAvailable()){
			if(parser.getDepDelay() > 0){
				
				//출력키 설정 : 출발지, 요일
				outputKey.setOrigin(parser.getOrigin());
				outputKey.setDate(parser.getDayofMonth());
				
				//출력값 설정 : 지연건수, 지연시간
				valueTime.set(parser.getDepDelay());
				outputValue.set(valueCount +","+valueTime);
				context.write(outputKey, outputValue);
				log.info("[monthly maper] outputKey = "+ outputKey + " | outputValue = " + outputValue);
				
			} else if(parser.getDepDelay() ==0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if(parser.getDepDelay() < 0){
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		} 
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
	
	}
}
