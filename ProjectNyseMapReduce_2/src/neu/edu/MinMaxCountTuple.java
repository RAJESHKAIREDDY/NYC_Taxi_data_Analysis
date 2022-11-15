package neu.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable{
	private String MaxPickUpTime = null;
	private String MinPickUpTime = null;
	private long count  = 0;
	public String getMaxPickUpTime() {
		return MaxPickUpTime;
	}
	public void setMaxPickUpTime(String maxPickUpTime) {
		MaxPickUpTime = maxPickUpTime;
	}
	public String getMinPickUpTime() {
		return MinPickUpTime;
	}
	public void setMinPickUpTime(String minPickUpTime) {
		MinPickUpTime = minPickUpTime;
	}

	
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		MaxPickUpTime = in.readUTF();
		MinPickUpTime = in.readUTF();
		count= in.readLong();
				
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(MaxPickUpTime);
		out.writeUTF(MinPickUpTime);
		out.writeLong(count);
		
		
	}
	@Override
	public String toString() {
		return "[MaxPickUpTime=" + MaxPickUpTime + ", MinPickUpTime=" + MinPickUpTime
				+ ", count=" + count + "]";
	}
}
