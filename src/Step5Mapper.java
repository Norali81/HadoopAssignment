import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// declare class
public class Step5Mapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
	
	// overwrite map function method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// transform "value" from type Text to String
		String line = value.toString();
		//Take a line from the text file and split it by empty space. 
		String[] lineSplit = line.split("	");
			//Write to context
			context.write(new FloatWritable(Float.parseFloat(lineSplit[1])), new Text(lineSplit[0]));
		
	}
	

}
