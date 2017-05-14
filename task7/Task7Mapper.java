package mapreduce.demo.task7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task7Mapper extends Mapper<LongWritable, Text, MonthYear, IntWritable> {
		
	MonthYear outKey;
	IntWritable temp;
	
	@Override
	public void setup(Context context) {
		outKey = new MonthYear();
		temp = new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		int month = Integer.parseInt(lineArray[0].split("-")[1]);
		int year = Integer.parseInt(lineArray[0].split("-")[2]);
		temp.set(Integer.parseInt(lineArray[2]));
		
		outKey.set(month, year);
		
		context.write(outKey, temp);
	}
}
