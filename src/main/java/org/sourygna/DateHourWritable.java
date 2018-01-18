package org.sourygna;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.IllegalArgumentException;

import org.apache.hadoop.io.WritableComparable;

public class DateHourWritable implements WritableComparable<DateHourWritable> {
	
	public enum Months {Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec};
	
	String hour;
	String day;
	String month;
	String year;
	
	public void setDate (String hour, String day, String month, String year) throws IllegalArgumentException{
		
		this.hour = hour;
		this.day = day;
		this.month = month;
		this.year = year;	
		
	}
	
	public Integer monthNumer(){
		return Months.valueOf(month).ordinal()+1;
	}
	
    public String getDate(){
		return "[" + day + "/" + this.monthNumer() + "/" + year + "-" + hour + "]";
		
	}
	
	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	@Override
	public void write(DataOutput out) throws IOException {	
		out.writeUTF(hour);	
		out.writeUTF(day);	
		out.writeUTF(month);	
		out.writeUTF(year);	;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.hour = in.readUTF();
		this.day = in.readUTF();;
		this.month = in.readUTF();
		this.year = in.readUTF();	
	}

	@Override
	public int compareTo(DateHourWritable o) {
		
		if (this.year.compareTo(o.getYear())==0){
			if (this.monthNumer()==o.monthNumer()){
				if (this.day.compareTo(o.getDay())==0){
					if (this.hour.compareTo(o.getHour())==0){
						return 0;
					} else {
						return this.hour.compareTo(o.getHour());	
					}
				} else {
					return this.day.compareTo(o.getDay());
				}
			} else {
				return this.monthNumer()-o.monthNumer();
			}
		} 
		return this.year.compareTo(o.getYear());
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((hour == null) ? 0 : hour.hashCode());
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DateHourWritable other = (DateHourWritable) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (hour == null) {
			if (other.hour != null)
				return false;
		} else if (!hour.equals(other.hour))
			return false;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[" + day + "/" + this.monthNumer() + "/" + year + "-" + hour + "]	";
	}
	



}