package neu.edu;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Set;
//import java.util.SortedMap;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>{
public Map<String , Integer > map = new  LinkedHashMap<String, Integer>();
	@Override
	protected void reduce(Text Pickup_Street, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable val:values)
		{
			sum =sum+val.get();
		}
		map.put(Pickup_Street.toString() , sum);
		//context.write(Pickup_Location, new IntWritable(sum));
	}

	public void cleanup(Context context){ 
	    //Cleanup is called once at the end to finish off anything for reducer
	    //Here we will write our final output
	    Map<String , Integer>  sortedMap = new HashMap<String , Integer>();    
	    sortedMap = sortMap(map);
	    
	    for (Map.Entry<String,Integer> entry : sortedMap.entrySet()){
	        try {
				context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}
	public Map<String , Integer > sortMap (Map<String,Integer> unsortMap){
	    Map<String ,Integer> hashmap = new LinkedHashMap<String,Integer>();
	    int count=0;
	    
	    List<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>(unsortMap.entrySet());
	    //Sorting the list we created from unsorted Map
	    Collections.sort(list , new Comparator<Map.Entry<String,Integer>>(){
	        public int compare (Map.Entry<String , Integer> o1 , Map.Entry<String , Integer> o2 ){
	            //sorting in descending order
	            return o2.getValue().compareTo(o1.getValue());
	        }
	    });

	    for(Map.Entry<String, Integer> entry : list){
	        // only writing top 5 in the sorted map 
	        if(count>6)
	            break;
	        hashmap.put(entry.getKey(),entry.getValue());
	    }
	    return hashmap ; 
	}
	

}
