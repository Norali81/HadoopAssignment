import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * This is the driver that starts all jobs in the “Wiki-PageCount Hoover”
 */
public class AllJobs extends Configured implements Tool{
	// setting output path
	// code onn chaining jobs obtained from
	// from http://unmeshasreeveni.blogspot.ie/2014/04/chaining-jobs-in-hadoop-mapreduce.html

	
	// Setting up variables for input and output paths
	// input path - this is where the data comes from
	private static final String JOB_1_IN_PATH = "wiki";
	// output  paths
	private static final String JOB_1_OUT_PATH = "out1_tmp_count";
	private static final String JOB_2_OUT_PATH = "out2_count_sorted";
	private static final String JOB_3_OUT_PATH = "out3_tmp_lng";
	private static final String JOB_4_OUT_PATH = "out4_tmp_lng_counts";
	private static final String JOB_5_OUT_PATH = "out5_avg_count_by_lng";

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Job 1
		 * Merge the files and sum up page counts
		 */
		 
		// configuration 
		Configuration conf = getConf();
		// create job
		Job job = Job.getInstance(conf, "Job1");
		
		// configure job
		job.setJarByClass(AllJobs.class);
		job.setMapperClass(Step1Mapper.class);
		job.setReducerClass(Step1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set file paths
		FileInputFormat.addInputPath(job, new Path(JOB_1_IN_PATH));
		FileOutputFormat.setOutputPath(job, new Path(JOB_1_OUT_PATH));
		job.waitForCompletion(true);
		
		/*
		 * Job 2
		 * Sort files by page count
		 */

		// create job
		Job job2 = Job.getInstance(conf);
		
		// configure job
		job2.setJarByClass(AllJobs.class);
		job2.setSortComparatorClass(Step2Comparator.class);
		job2.setMapperClass(Step2Mapper.class);
		job2.setReducerClass(Step2Reducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		// set up file paths, wait for completion
		FileInputFormat.addInputPath(job2, new Path(JOB_1_OUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(JOB_2_OUT_PATH));
		job2.waitForCompletion(true);
		
		/*
		 * Job 3
		 * Extract language, count and page
		 */
		
		// configure job
		Job job3 = Job.getInstance(conf); 
		job3.setJarByClass(AllJobs.class);
		job3.setMapperClass(Step3Mapper.class);
		job3.setReducerClass(Step3Reducer.class);
		
		// set specific comparators for secondary sort
		job3.setSortComparatorClass(KeyComparator.class);
		job3.setGroupingComparatorClass(GroupComparator.class);
				
		// set output class to our custom CountLangPair class, for secondary sort
		job3.setOutputKeyClass(CountLangPair.class);
		job3.setOutputValueClass(Text.class);
		
		// set file paths
		FileInputFormat.addInputPath(job3, new Path(JOB_1_IN_PATH));
	    FileOutputFormat.setOutputPath(job3, new Path(JOB_3_OUT_PATH));
	    
	    // wait for completion
	    job3.waitForCompletion(true);
	    
	    // get counters defined in the mapper class
	    Counters counters = job3.getCounters(); 
	    Counter records = counters.findCounter(Step3Mapper.MyCounter.Records); 
	    // print counter result 
	    System.out.println("Number of records processed: " + records);
	    
	    /*
	     * Job 4
	     * strip page out of file and reverse order to put combined key in front
	     */
	    
	    // configure job
	    Job job4 = Job.getInstance(conf);
		job4.setJarByClass(AllJobs.class);
		job4.setMapperClass(Step4Mapper.class);
		job4.setReducerClass(Step4Reducer.class);
		
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);
		
		// set file paths
		FileInputFormat.addInputPath(job4, new Path(JOB_3_OUT_PATH));
	    FileOutputFormat.setOutputPath(job4, new Path(JOB_4_OUT_PATH));
	    
	    //wait for completion
	    job4.waitForCompletion(true);
	    
	    /*
	     * Job 5
	     * Sort by page count
	     */
	    
	    // configure job
	    Job job5 = Job.getInstance(conf);
		job5.setJarByClass(AllJobs.class);
		job5.setMapperClass(Step5Mapper.class);
		job5.setReducerClass(Step5Reducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(FloatWritable.class);
		job5.setOutputKeyClass(FloatWritable.class);
		job5.setOutputValueClass(Text.class);
		
		// set comparator classes
		job5.setSortComparatorClass(Step2Comparator.class);
		job5.setGroupingComparatorClass(Step2Comparator.class);

		// set file paths
		FileInputFormat.addInputPath(job5, new Path(JOB_4_OUT_PATH));
		FileOutputFormat.setOutputPath(job5, new Path(JOB_5_OUT_PATH));
		
		// wait for completion and return true if finished
		job5.waitForCompletion(true);
	    return job5.waitForCompletion(true) ? 0 : 1;
	    
		
		

	}

	public static void main(String[] args2) throws Exception {
		// use Toolrunner to run all jubs
		ToolRunner.run(new Configuration(), new AllJobs(), args2);

	}

}
