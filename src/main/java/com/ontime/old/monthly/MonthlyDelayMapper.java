package com.ontime.old.monthly;

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
public class MonthlyDelayMapper extends Mapper<LongWritable, Text, MonthlyComplexKey, IntWritable> {
	
	private MonthlyComplexKey outputKey = new MonthlyComplexKey();
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
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
				log.info("mapper D outputKey = "+ outputKey + ", outputValue = "+ outputValue);
			} else if(parser.getDepDelay() ==0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if(parser.getDepDelay() < 0){
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}

		// 도착 지연인 경우
		if(parser.isArrDelayAvailable()){
			if(parser.getArrDelay() > 0){
				outputKey.setOrigin("A," + parser.getOrigin());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
				log.info("mapper A outputKey = "+ outputKey + ", outputValue = "+ outputValue);
			} else if(parser.getArrDelay() ==0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if(parser.getArrDelay() < 0){
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	
	}
}
