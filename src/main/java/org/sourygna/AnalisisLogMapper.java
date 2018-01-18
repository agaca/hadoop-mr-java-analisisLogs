package org.sourygna;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;



public class AnalisisLogMapper extends Mapper<LongWritable, Text, DateHourWritable, Text> {
	
	private Map<DateHourWritable,Map<String,Integer>> buffer;
	private DateHourWritable logDate;
	private Text outCompEvents = new Text();


	protected void setup(
			Mapper<LongWritable, Text, DateHourWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		buffer = new HashMap<DateHourWritable, Map<String,Integer>>();
	}
	
	protected void map(LongWritable offset, Text line,
			Mapper<LongWritable, Text, DateHourWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		
			String [] logEntry = line.toString().split(" ");
			String month = logEntry[0];
			String day = logEntry[1];
			String hour = logEntry[2].substring(0, 2);
			String year = "2014";
			logDate = new DateHourWritable();
			logDate.setDate(hour,day,month,year);
		
			String comp = logEntry[4].replaceAll("\\[.*?\\] ?", "");
			
			if (!comp.startsWith("vmnet")){
				Counter counter= context.getCounter("LogComponent", comp);
				counter.increment(1);
				Integer countComp;
				if(buffer.containsKey(logDate)){
					Map<String, Integer> compNum; 
					compNum = buffer.get(logDate);
					if(compNum.containsKey(comp)){
						countComp = compNum.get(comp);
						countComp++;			
					} else {
						countComp = 1;	
					}
					compNum.put(comp, countComp);
					buffer.put(logDate, compNum);
				} else {
					countComp = 1;
					Map<String, Integer> newMapCompNum = new HashMap<String, Integer>();
					newMapCompNum.put(comp, countComp);
					buffer.put(logDate, newMapCompNum);
				}
			}
	}


	protected void cleanup(
			Mapper<LongWritable, Text, DateHourWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		for (Entry<DateHourWritable,Map<String,Integer>> entryBuffer: buffer.entrySet()){
			ArrayList<String> temp = new ArrayList<String>();
			for (Entry<String, Integer> entryComp : entryBuffer.getValue().entrySet()) {	
				
				temp.add(entryComp.getKey() + entryComp.getValue());
				
			}
			String formatOutString = temp.toString().replaceAll("\\[|\\s|\\]", "");
			outCompEvents.set(formatOutString);
			context.write(entryBuffer.getKey(),outCompEvents);
		}
		
	}
}