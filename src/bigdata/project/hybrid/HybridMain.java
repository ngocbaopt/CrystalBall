package bigdata.project.hybrid;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author baopham
 *
 *         In this project you will create a crystal ball to predict events that
 *         may happen once a certain event happened.
 * 
 *         Example: Amazon will say people who bought “item one” have bought the
 *         following items : “item two”, “item three”, “item four”.
 * 
 *         For the purpose of this project you can assume that historical
 *         customer data is available in the following form.
 * 
 *         34 56 29 12 34 56 92 10 34 12 // items bought by a customer, listed
 *         in the order she bought it
 * 
 *         18 29 12 34 79 18 56 12 34 92 // items bought by another customer,
 *         listed in the order she bought it
 * 
 *         Let the neighborhood of X, N(X) be set of all term after X and before
 *         the next X.
 * 
 *         Example: Let Data block be [a b c a d e]
 * 
 *         N(a) = {b, c}, N(b) = {c, a, d, e}, N(c) = {a, d, e}, N(a) ={d, e},
 *         N(d) = {e}, N(e) = {}
 * 
 *         Part 4: Implement Pairs in Mapper and Stripes in Reducer to compute
 *         relative frequencies
 */

public class HybridMain extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(HybridMain.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.info("Starting RelativeFrequenciesByHybrid");
		int res = ToolRunner.run(new HybridMain(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "RelativeFrequenciesByHybrid");
		job.setJarByClass(this.getClass());
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(HybridMapper.class);
		job.setReducerClass(HybridReducer.class);
		
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
