package neu.edu;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public  class MinMaxReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
		
		result.setMinPickUpDate(null);
		result.setMaxPickUpDate(null);
		result.setCount(0);
		int sum = 0;
		
		for (MinMaxCountTuple val : values){
	
		
			if(result.getMinPickUpDate() == null ||
					val.getMinPickUpDate().compareTo(result.getMinPickUpDate())<0) {
				result.setMinPickUpDate(val.getMinPickUpDate());
				//System.out.println(result.getMinPickUpDate());
			}
			
			if(result.getMaxPickUpDate() == null ||
					val.getMaxPickUpDate().compareTo(result.getMaxPickUpDate())>0) {
				result.setMaxPickUpDate(val.getMaxPickUpDate());
			}
			sum +=val.getCount();
		}
		result.setCount(sum);
		context.write(key, result);
		}
}
