package neu.edu;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple>{
	private MinMaxCountTuple Output = new MinMaxCountTuple();
	private Text date = new Text();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyy");

	 @Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		 String val = value.toString();
		 if(val.contains("pickup_month")&& (val.contains("pickup_date"))){
			 return;
		 }
		 else {
			 String words[]= val.split(",");
			 String pickup_month = words[3];
			 Date pickup_date = null;
			try {
//				System.out.println("Current words 1");
//				System.out.println(words[1]);
				pickup_date = frmt.parse(words[1]);
//				System.out.println("Formatted date : ");
//				System.out.println(pickup_date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
			
			 Output.setMinPickUpDate(pickup_date);
			 Output.setMaxPickUpDate(pickup_date);
			 Output.setCount(1);
	
			 date.set(pickup_month);
			 System.out.println(Output);
			 context.write(date, Output);
	
			 }
		
	}
}
