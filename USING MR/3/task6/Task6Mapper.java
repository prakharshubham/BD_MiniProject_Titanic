//No of people alive per gender
package mapreduce.demo.task6;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Task6Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	//IntWritable class_travelled;
	IntWritable living;
	//Text embarked;
	IntWritable qty;
	Text gender;
	
	@Override
	public void setup(Context context){
		//class_travelled=new IntWritable();
		living=new IntWritable();
		gender = new Text();
		//embarked = new Text();
		qty=new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		String[] lineArray=value.toString().split(",");
		living.set(Integer.parseInt(lineArray[1]));
		//class_travelled.set(Integer.parseInt(lineArray[2]));
		//embarked.set(lineArray[11]);
		
		if(living.equals(1))
		{
			//class_travelled.set(Integer.parseInt(lineArray[2]));
			gender.set(lineArray[4]);
			qty.set(1);
			context.write(gender,qty);
		}
		
	
	}
	
	

}
