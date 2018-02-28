import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Reducer: Just merge and output the results from the mapper
 */
public class Step2Reducer extends Reducer <IntWritable, Text, Text, Text>{
	
	public void reduce(Iterable<IntWritable> key, Text values, Context context) throws IOException, InterruptedException{

		String line = values.toString();
		context.write(new Text(key.toString()), new Text(line));
	
	}

}
