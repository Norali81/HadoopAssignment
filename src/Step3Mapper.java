import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Step 3 strip page size and reverse order of fields
 */
// declare class
public class Step3Mapper extends Mapper<LongWritable, Text, CountLangPair, Text> {
	
	// declare a counter
	static enum MyCounter{Records}; 
	
	// overwrite map function method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		System.out.println("We are in the mapper now!");
		// transform "value" from type Text to String
		String line = value.toString();
		//Take a line from the text file and split it by empty space. 
		String[] lineSplit = line.split(" ");
		if (lineSplit.length>=3){
			// assign the split text to different variables
			Text language = new Text(lineSplit[0]);
			IntWritable count = new IntWritable(Integer.parseInt(lineSplit[2]));
			Text wikiPage = new Text(lineSplit[1]);
			// create an instance of a key pair for secondary sort
			CountLangPair pair = new CountLangPair(count, language);
			// write the key pair and the pair to context 
			context.write(pair, wikiPage);
			
			// increment counter
			context.getCounter(MyCounter.Records).increment(1);
		} else {
			System.out.println("Line not counted. Malformatted");
		}
		
	}
	

}
