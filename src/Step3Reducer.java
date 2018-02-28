import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Reducer: Just merge files and write to context
 */
public class Step3Reducer extends Reducer<CountLangPair, Text, CountLangPair, Text>{
	
	public void reduce(CountLangPair key, Text values, Context context) throws IOException, InterruptedException{
		System.out.println("We are in the reducer now");
		int count=0;
		
		context.write(key, new Text());
	}

}
