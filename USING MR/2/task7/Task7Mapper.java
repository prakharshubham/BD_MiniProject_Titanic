// Calculating people alive per class who embarked at S
package mapreduce.demo.task7;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Task7Mapper extends Mapper<LongWritable,IntWritable,IntWritable,IntWritable>{
	
	IntWritable class_travelled;
	IntWritable living;
	Text embarked;
	IntWritable qty;
	//Text gender;
	
	@Override
	public void setup(Context context){
		class_travelled=new IntWritable();
		living=new IntWritable();
		//gender = new Text();
		embarked = new Text();
		qty=new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException{
	
		String[] lineArray=value.toString().split(",");
		living.set(Integer.parseInt(lineArray[1]));
		class_travelled.set(Integer.parseInt(lineArray[2]));
		embarked.set(lineArray[11]);
		
		
		if(living.equals(0)&& embarked.equals('S'))
		{
			//class_travelled.set(Integer.parseInt(lineArray[2]));
			//gender.set(lineArray[4]);
			qty.set(1);
			context.write(class_travelled,qty);
		}
		
	
	}
	
	

}
