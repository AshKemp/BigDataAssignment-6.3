package mapreduce.demo.task7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
 
public class MonthYear implements WritableComparable<MonthYear> {
 
    private int month;
    private int year;
 
    public int getMonth() {
        return month;
    }
 
    public int getYear() {
        return year;
    }
 
    public void set(int month, int year) {
        this.month = month;
        this.year = year;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
    	month = in.readInt();
    	year = in.readInt();
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(month);
    	out.writeInt(year);
    }
 
    @Override
    public String toString() {
        return month + "\t" + year;
    }
 
    @Override
    public int compareTo(MonthYear monthYear) {
        int cmp = (year - monthYear.getYear());
 
        if (cmp != 0) {
            return cmp;
        }
 
        return (month - monthYear.getMonth());
    }
 
    @Override
    public int hashCode(){
        return year;
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof MonthYear)
        {
        	MonthYear monthYear = (MonthYear) o;
            return (year == monthYear.getYear() && month == monthYear.getMonth());
        }
        return false;
    }
  
}