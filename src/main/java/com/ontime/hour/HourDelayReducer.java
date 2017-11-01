package com.ontime.hour;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lombok.extern.java.Log;

@Log
public class HourDelayReducer extends Reducer<HourComplexKey, IntWritable, HourComplexKey, IntWritable> {
	
//	private MultipleOutputs<Text, IntWritable> multiKey;
//	private Text outputKey = new Text();

	private MultipleOutputs<HourComplexKey, IntWritable> multiKey;
	private HourComplexKey outputKey = new HourComplexKey();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
//		multiKey = new MultipleOutputs<Text, IntWritable>(context);
		multiKey = new MultipleOutputs<HourComplexKey, IntWritable>(context);
	}
	
	@Override
	protected void reduce(HourComplexKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin 
		String[] columns = key.getOrigin().toString().split(",");

		int sum =0;
		Integer bHour = key.getHour();
		
		//출발 지연인 경우
		if(columns[0].equals("D")){
			for(IntWritable value : values){
				if(bHour !=key.getHour()){
					outputValue.set(sum);
//					outputKey.set(key.getOrigin().substring(2)+","+bHour+",");
					outputKey.setHour(bHour);
					multiKey.write("hourDeparture", outputKey, outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bHour = key.getHour();
			}
			
			if(key.getHour() == bHour){
//				outputKey.set(key.getOrigin().substring(2)+","+key.getHour()+",");
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setHour(key.getHour());
				outputValue.set(sum);
				multiKey.write("hourDeparture", outputKey, outputValue);	
			}
			
		} else {
		//도착 지연인 경우
			for(IntWritable value : values){
				if(bHour !=key.getHour()){
					outputValue.set(sum);
//					outputKey.set(key.getOrigin().substring(2)+","+bHour+",");
					outputKey.setOrigin(key.getOrigin().substring(2));
					outputKey.setHour(bHour);
					multiKey.write("hourArrival", outputKey, outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bHour = key.getHour();
			}
			
			if(key.getHour() == bHour){
//				outputKey.set(key.getOrigin().substring(2)+","+key.getHour()+",");
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setHour(key.getHour());
				outputValue.set(sum);
				multiKey.write("hourArrival", outputKey, outputValue);	
			}
		}
			
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multiKey.close();
	}
}
