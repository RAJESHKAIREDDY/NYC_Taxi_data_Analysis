package neu.edu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{
		private Text Pickup_Street = new Text();
	
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line=value.toString();
			if(line.contains("PickUp_Street")) {
				return;
			}
			else {
				
				String words[]=line.split(",");
				Pickup_Street.set(words[13]);
				context.write(new Text(Pickup_Street), new IntWritable(1));
				//System.out.println(Pickup_Location);
				}
		}
	

}
