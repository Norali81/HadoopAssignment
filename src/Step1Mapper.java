import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Step 1 mapper. Takes input file one line at a time, 
 * writes page and count to the output for reducer. 
 */
// declare class
public class Step1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// overwrite map function method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// transform "value" from type Text to String
		String line = value.toString();
		//Take a line from the text file and split it by empty space. 
		String[] lineSplit = line.split(" ");
		if (lineSplit.length>=3){
			// write text and number to context for reducer to use
			context.write(new Text(lineSplit[1]),new IntWritable(Integer.parseInt(lineSplit[2])));
		} else {
			System.out.println("Line not counted. Malformatted");
		}
		
	}
	

}
