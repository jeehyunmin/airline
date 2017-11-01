package com.ontime.origin.month;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import lombok.extern.java.Log;

@Log
public class MonthDelayReducer extends Reducer<MonthComplexKey, Text, MonthComplexKey, Text> {
	
	private MonthComplexKey outputKey = new MonthComplexKey();
	private Text outputValue = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		log.info("reducer started");
	}
	
	@Override
	protected void reduce(MonthComplexKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//key로 들어오는 값이 "D" or "A",origin 
//		String[] columns = key.getOrigin().toString().split(",");
		Integer bMonth = key.getMonth();

		//values
		int countSum =0;				//지연 횟수 구하기
		double timeSum =0.0;			//지연 시간의 합 구하기
		double timeAvg =0.0;			//지연 시간의 평균 구히기
		
		int countValue =0;
		double timeValue =0.0;
		
			for(Text value : values){
				String[] inputValue = value.toString().split(",");
				
				if(bMonth !=key.getMonth()){
					timeAvg = timeSum/countSum;
					outputValue.set(","+ countSum+","+ timeAvg);
					outputKey.setOrigin(key.getOrigin());
					outputKey.setMonth(bMonth);
					context.write(outputKey, outputValue);
					
					log.info("[for-in-if-context.write] outputKey = "+ outputKey+", outputValue = " + outputValue);
					
					countSum =0; 			//초기화
					timeSum =0.0;
					timeAvg =0.0;
				}
				countValue = Integer.parseInt(inputValue[0]);
				timeValue = Double.parseDouble(inputValue[1]);
				
				countSum += countValue;
				timeSum += timeValue;
				bMonth = key.getMonth();
			}
			
			if(key.getMonth() == bMonth){
				outputKey.setOrigin(key.getOrigin());
				outputKey.setMonth(key.getMonth());
				timeAvg = timeSum/countSum;
				outputValue.set(","+ countSum+","+ timeAvg);
				context.write(outputKey, outputValue);	
				log.info("[for-out-if-context.write] outputKey = "+ outputKey+", outputValue = " + outputValue);
			}
			
		}
}
