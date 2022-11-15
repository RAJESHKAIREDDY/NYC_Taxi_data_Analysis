package neu.edu;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class MinMaxReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
		
		result.setMinPickUpTime(null);
		result.setMaxPickUpTime(null);
		result.setCount(0);
		int sum = 0;
		
		for(MinMaxCountTuple val: values)
		{
			if(result.getMinPickUpTime()== null || val.getMinPickUpTime().compareTo(result.getMinPickUpTime())< 0) {
				result.setMinPickUpTime(val.getMinPickUpTime());
			}
			if(result.getMaxPickUpTime()== null || val.getMaxPickUpTime().compareTo(result.getMaxPickUpTime())>0) {
				
				result.setMaxPickUpTime(val.getMaxPickUpTime());
			}
			
			sum += val.getCount();
			
		}
		result.setCount(sum);
		context.write(key, result);
	}

}
