package neu.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountaverageTuple  implements Writable{
		private float count = 0;
		private float average = 0;
		public float getCount() {
			return count;
		}
		public void setCount(float count) {
			this.count = count;
		}
		public float getAverage() {
			return average;
		}
		public void setAverage(float average) {
			this.average = average;
		}
		
		
		@Override
		public void readFields(DataInput in) throws IOException {
		 count = in.readFloat();
		 average = in.readFloat();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(count);
			out.writeFloat(average);
		}
	
		@Override
		public String toString() {
			//return "AverageDistance [count=" + count + ", average=" + average + "]";
			return "AverageDistance [ average=" + average + ", count=" + count + "]";
		}

}