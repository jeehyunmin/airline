package com.ontime.weekend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lombok.extern.java.Log;

@Log
public class WeekendDelayReducer extends Reducer<WeekendComplexKey, IntWritable, WeekendComplexKey, IntWritable> {
	
	private MultipleOutputs<WeekendComplexKey, IntWritable> multiKey;
	private WeekendComplexKey outputKey = new WeekendComplexKey();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
		multiKey = new MultipleOutputs<WeekendComplexKey, IntWritable>(context);
	}
	
	@Override
	protected void reduce(WeekendComplexKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin 
		String[] columns = key.getOrigin().toString().split(",");

		int sum =0;
		Integer bWeekend = key.getWeekend();
		
		//출발 지연인 경우
		if(columns[0].equals("D")){
			for(IntWritable value : values){
				if(bWeekend !=key.getWeekend()){
					outputValue.set(sum);
					outputKey.setWeekend(bWeekend);
					multiKey.write("departure", outputKey, outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bWeekend = key.getWeekend();
			}
			
			if(key.getWeekend() == bWeekend){
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setWeekend(key.getWeekend());
				outputValue.set(sum);
				multiKey.write("departure", outputKey, outputValue);	
			}
			
		} else {
		//도착 지연인 경우
			for(IntWritable value : values){
				if(bWeekend !=key.getWeekend()){
					outputValue.set(sum);
					outputKey.setOrigin(key.getOrigin().substring(2));
					outputKey.setWeekend(bWeekend);
					multiKey.write("arrival", outputKey, outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bWeekend = key.getWeekend();
			}
			
			if(key.getWeekend() == bWeekend){
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setWeekend(key.getWeekend());
				outputValue.set(sum);
				multiKey.write("arrival", outputKey, outputValue);	
			}
		}
			
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multiKey.close();
	}
}
