import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Strip page and reverse order to make the language key
 */
// declare class
public class Step4Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {


	// overwrite map function method
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("We are in the mapper now!");
		// transform "value" from type Text to String
		String line = value.toString();
		// Take a line from the text file and split it by empty space.
		String[] lineSplit = line.split("	");
		if (lineSplit.length >= 3) {
			//write language and count to context
			IntWritable count = new IntWritable(Integer.parseInt(lineSplit[0]));
			Text language = new Text(lineSplit[1]);
			Text wikiPage = new Text(lineSplit[2]);
			context.write(language, count);

		}

	}
}
