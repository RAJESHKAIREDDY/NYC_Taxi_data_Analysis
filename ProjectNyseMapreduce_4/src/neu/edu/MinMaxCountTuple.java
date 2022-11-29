package neu.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable{
	private Date MaxPickUpDate = new Date();
	private Date MinPickUpDate = new Date();
	private long count = 0;
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyy");
	
	public Date getMaxPickUpDate() {
		return MaxPickUpDate;
	}
	public void setMaxPickUpDate(Date maxPickUpDate) {
		MaxPickUpDate = maxPickUpDate;
	}
	public Date getMinPickUpDate() {
		return MinPickUpDate;
	}
	public void setMinPickUpDate(Date minPickUpDate) {
		MinPickUpDate = minPickUpDate;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		MaxPickUpDate = new Date(in.readLong());
		MinPickUpDate = new Date(in.readLong());
		count = in.readLong();
				
	}
	@SuppressWarnings("deprecation")
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(MaxPickUpDate.getDate());
		out.writeLong(MinPickUpDate.getDate());
		out.writeLong(count);
		
		
	}
	@Override
	public String toString() {
		return frmt.format(MaxPickUpDate) + "\t" + frmt.format(MinPickUpDate) + "\t" + count;
	}
}
