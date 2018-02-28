import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Step 1 reducer. Sums up all page counts by page
 */
public class Step1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	// overwrite reduce method
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		//initiate counter
		int count=0;
		// for each sum up all the values (page counts) belonging to a specific key (page)
		for (IntWritable value : values) {
			count = count + value.get();
        }
		// write page and count to context
		context.write(key, new IntWritable(count));
	}

}
