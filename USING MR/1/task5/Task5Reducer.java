package mapreduce.demo.task5;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Task5Reducer extends Reducer<Text, FloatWritable,Text, FloatWritable> {

	FloatWritable outValue=new FloatWritable();
	public void reduce (Text key, Iterable<FloatWritable> values, Context context)
	throws IOException, InterruptedException{
		float sum=0;
		float nop=0;
		for(FloatWritable value:values){
			sum+=value.get();
			nop=nop+1;
			
		}
		float avg=sum/nop;
		outValue.set(avg);
		context.write(key, outValue);
		
		
	}
}
