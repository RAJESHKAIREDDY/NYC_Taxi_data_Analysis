package neu.edu;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text,CountaverageTuple,Text,CountaverageTuple>{
	private CountaverageTuple res = new CountaverageTuple();
	
	
	
	@Override
	protected void reduce(Text key, Iterable<CountaverageTuple> values,Context context)
			throws IOException, InterruptedException {
		float  sum = 0;
		float counter = 0;
	
		for(CountaverageTuple val:values)
		{
			sum+=val.getCount() * val.getAverage();
			counter+= val.getCount();
		}
		 
		res.setCount(counter);
		res.setAverage(sum/counter);
		
		
		context.write(key, res);
	}

}