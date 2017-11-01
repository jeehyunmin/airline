package com.ontime.old.monthly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lombok.extern.java.Log;

@Log
public class MonthlyDelayReducer extends Reducer<MonthlyComplexKey, IntWritable, MonthlyComplexKey, IntWritable> {
	
	private MultipleOutputs<MonthlyComplexKey, IntWritable> multiKey;
	private MonthlyComplexKey outputKey = new MonthlyComplexKey();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
		multiKey = new MultipleOutputs<MonthlyComplexKey, IntWritable>(context);
	}
	
	@Override
	protected void reduce(MonthlyComplexKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin 
		String[] columns = key.getOrigin().toString().split(",");

		int sum =0;
		Integer bMonth = key.getMonth();
		
		//출발 지연인 경우
		if(columns[0].equals("D")){
			for(IntWritable value : values){
				if(bMonth !=key.getMonth()){
					outputValue.set(sum);
					outputKey.setOrigin(key.getOrigin().substring(2));
					outputKey.setMonth(bMonth);
					multiKey.write("monthDeparture", outputKey, outputValue);
					log.info("reducer D: "+"outputKey = "+ outputKey + ", outputValue = "+ outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bMonth = key.getMonth();
			}
			
			if(key.getMonth() == bMonth){
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setMonth(key.getMonth());
				outputValue.set(sum);
				multiKey.write("monthDeparture", outputKey, outputValue);
				log.info("reducer D: "+"outputKey = "+ outputKey + ", outputValue = "+ outputValue);
			}
			
		} else {
		//도착 지연인 경우
			for(IntWritable value : values){
				if(bMonth !=key.getMonth()){
					outputValue.set(sum);
					outputKey.setOrigin(key.getOrigin().substring(2));
					outputKey.setMonth(bMonth);
					multiKey.write("monthArrival", outputKey, outputValue);
					log.info("reducer A: "+"outputKey = "+ outputKey + ", outputValue = "+ outputValue);
					sum =0; 			//초기화
				}
				
				sum += value.get();
				bMonth = key.getMonth();
			}
			
			if(key.getMonth() == bMonth){
				outputKey.setOrigin(key.getOrigin().substring(2));
				outputKey.setMonth(key.getMonth());
				outputValue.set(sum);
				multiKey.write("monthArrival", outputKey, outputValue);	
				log.info("reducer A: "+"outputKey = "+ outputKey + ", outputValue = "+ outputValue);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multiKey.close();
	}
}
