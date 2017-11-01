package com.ontime.carrier;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lombok.extern.java.Log;

@Log
public class CarrierDelayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private MultipleOutputs<Text, IntWritable> multiKey;
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
		multiKey = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin,건수 
				String[] columns = key.toString().split(",");

				//출력키 : outputKey  = origin
				outputKey.set(columns[1]);
				
				//출발 지연인 경우
				if(columns[0].equals("D")){
					int sum = 0;
					for(IntWritable value : values){
						sum += value.get();
					}
					outputValue.set(sum);
					multiKey.write("departure", outputKey, outputValue);
				} else {
				//도착 지연인 경우
					int sum = 0;
					for(IntWritable value : values){
						sum += value.get();
					}
					outputValue.set(sum);
					multiKey.write("arrival", outputKey, outputValue);
				}
			}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multiKey.close();
	}
}
