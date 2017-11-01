package com.ontime.origin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lombok.extern.java.Log;

@Log
public class OriginDelayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin,건수 
		String[] columns = key.toString().split(",");

		//출력키 : outputKey  = origin
		outputKey.set(columns[1]+",");
		
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			outputValue.set(sum);
			context.write(outputKey, outputValue);
			log.info("[OriginDelayReducer] outputKey = "+outputKey+", outputValue = " +outputValue);
	}
}
