package neu.edu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	
	String iPath = "hdfs://localhost:9000/Project/MapReduce1/Input/hive_dataset.csv";
	String oPath = "hdfs://localhost:9000/Project/MapReduce1/Output";
	
	Path inputPath =  new Path(iPath);
	Path outputPath =  new Path(oPath);
	
	Configuration conf= new Configuration();
	Job job = Job.getInstance(conf);
	
	job.setMapperClass(MinMaxCountMapper.class);
	job.setReducerClass(MinMaxReducer.class);
	job.setJarByClass(Driver.class);
	
	//job.setNumReduceTasks(0);
	
	/*key-value pair output from the mapper*/
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(MinMaxCountTuple.class);
	

	 
	//Configuring the input/output path from the file system into the job
	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job,outputPath);	
	
	/* hadoop does not allow u if the output path/file exists. so we have to delete the file before creating a path/file*/
	outputPath.getFileSystem(conf).delete(outputPath, true);
	
	/*if job get successful it returns 0 and 1 if it fails*/
	System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}
