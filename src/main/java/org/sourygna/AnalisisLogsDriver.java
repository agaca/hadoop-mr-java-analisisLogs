package org.sourygna;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;


public class AnalisisLogsDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: Log Analisis <input dir> <output dir>\n");
			System.exit(-1);
		}

		String input = args[0];
		String output = args[1];

		Job job = Job.getInstance();
		job.setJarByClass(AnalisisLogsDriver.class);
		job.setJobName("Log Analisis");
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(AnalisisLogMapper.class);
		job.setMapOutputKeyClass(DateHourWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setReducerClass(AnalisisLogReducer.class);
		job.setOutputKeyClass(DateHourWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		

		boolean success = job.waitForCompletion(true);
	
		CounterGroup counters = job.getCounters().getGroup("LogComponent");
	    for(Counter counter:counters){
	    	System.out.println(counter.getDisplayName() + counter.getValue());
	    }
	    
		System.exit(success ? 0 : 1);
		
	}
}