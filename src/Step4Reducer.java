import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Step 4 reducer: Sum up page counts
 */
public class Step4Reducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		// declare variables
		int pageCount=0;
		int records=0;
		float avgPageCount=0;
		
		//increment page count
		for (IntWritable value : values) {
			pageCount = pageCount + value.get();
			records++;
		
        }
		// calculate average page count
		avgPageCount = pageCount/records;
		
		// write average page count to context
		context.write(key,new FloatWritable(avgPageCount));
	}
}