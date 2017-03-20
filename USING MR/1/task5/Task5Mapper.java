package mapreduce.demo.task5;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class Task5Mapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
	
	FloatWritable fare;
	Text class_travelled;
	
	@Override
	public void setup(Context context){
		fare=new FloatWritable();
		class_travelled=new Text();
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
		
		String lineArray[]=value.toString().split(",");
		fare.set(Float.parseFloat(lineArray[9]));
		class_travelled.set(lineArray[2]);
		context.write(class_travelled,fare);
		
	}
}
