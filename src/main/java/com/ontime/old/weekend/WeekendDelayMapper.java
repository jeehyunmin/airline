package com.ontime.old.weekend;

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
public class WeekendDelayMapper extends Mapper<LongWritable, Text, WeekendComplexKey, IntWritable> {
	
	private WeekendComplexKey outputKey = new WeekendComplexKey();
	private IntWritable outputValue = new IntWritable(1);
	
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
				outputKey.setOrigin("D," + parser.getOrigin());
				outputKey.setWeekend(parser.getDayofWeek());
				context.write(outputKey, outputValue);
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
