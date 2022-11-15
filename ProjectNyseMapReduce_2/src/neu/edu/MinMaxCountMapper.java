package neu.edu;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple>{
	private MinMaxCountTuple Output = new MinMaxCountTuple();
	private Text date = new Text();
	
	 @Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		 String val = value.toString();
		 if(val.contains("pickup_time")&& (val.contains("pickup_month"))){
			 return;
		 }
		 else {
			 String words[]= val.split(",");
			 String  pickup_date = words[1];
			 String pickup_time = words[2];
			
			 
			 Output.setMinPickUpTime(pickup_time);
			 Output.setMaxPickUpTime(pickup_time);
			 Output.setCount(1);
			 
			 date.set(pickup_date);
			 
			 context.write(date, Output);
			 }
		
	}
}
