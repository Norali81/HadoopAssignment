import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Step 2 - Reverse key & value position and order by page count
 */
// declare class
public class Step2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	// overwrite map function method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// transform "value" from type Text to String
		String line = value.toString();
		//Take a line from the text file and split it by empty space. 
		String[] lineSplit = line.split("	");
			// write page count and page to context
			context.write(new IntWritable(Integer.parseInt(lineSplit[1])), new Text(lineSplit[0]));
		
	}
	

}
