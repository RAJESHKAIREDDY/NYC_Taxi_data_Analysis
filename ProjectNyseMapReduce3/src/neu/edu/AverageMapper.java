package neu.edu;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Object, Text, Text, CountaverageTuple>{
		private Text PickupLocation_Month = new Text();
		private CountaverageTuple AverageDistance = new CountaverageTuple();
		
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line=value.toString();
			if(line.contains("pickup_month")&& line.contains("trip_distance")) {
				return;
			}
			else {
				String words[]=line.split(",");
		
				PickupLocation_Month.set(words[3]);
				AverageDistance.setCount(1);
				AverageDistance.setAverage(Float.parseFloat(words[10]));
				//System.out.println(words[10]);
				//System.out.println(words[3]);
				context.write(new Text(PickupLocation_Month),AverageDistance);
			}
		}
}
	
