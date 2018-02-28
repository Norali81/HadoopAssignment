import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Just write the content to the reducer. 

public class Step5Reducer extends Reducer <FloatWritable, Text, Text, Text>{
	
	public void reduce(Iterable<IntWritable> key, Text values, Context context) throws IOException, InterruptedException{

		String line = values.toString();
		context.write(new Text(key.toString()), new Text(line));
	
	}

}
