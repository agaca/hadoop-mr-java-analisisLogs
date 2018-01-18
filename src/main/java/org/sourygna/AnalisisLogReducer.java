package org.sourygna;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalisisLogReducer extends
		Reducer<DateHourWritable, Text, DateHourWritable, Text> {

	private Text outputEvents = new Text();
	private Map<String, Integer> compCounterMap;
	
	@Override
	protected void reduce(
			DateHourWritable logDate,
			Iterable<Text> events,
			Reducer<DateHourWritable, Text, DateHourWritable, Text>.Context context)
			throws IOException, InterruptedException {


		String[] eventStringTemp, eventString;
		String comp;
		Integer numberComp;
		Integer numberCompTemp;
		
		compCounterMap = new HashMap<String, Integer>();
		
		for (Text eventList : events) {
			eventStringTemp = eventList.toString().split(",");
			for (String event : eventStringTemp) {
				eventString = event.split(":");
				comp = eventString[0];
				numberComp = Integer.parseInt(eventString[1]);
				if (compCounterMap.containsKey(comp)) {
					numberCompTemp = compCounterMap.get(comp);
					numberCompTemp += numberComp;
					compCounterMap.put(comp, numberCompTemp);
				} else {
					compCounterMap.put(comp, numberComp);

				}

			}
		}
		
		ArrayList<String> temp = new ArrayList<String>();
		for (Entry<String, Integer> entry : compCounterMap.entrySet()) {
			temp.add(entry.getKey() + ":" + entry.getValue());

		}
		String formatOutString = temp.toString().replaceAll("\\[|\\s|\\]", "");

		outputEvents.set(formatOutString);
		context.write(logDate, outputEvents);
	}

}