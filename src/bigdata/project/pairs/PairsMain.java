
package bigdata.project.pairs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 *         Part 2: Implement Pairs algorithm to compute relative frequencies
 */
public class PairsMain extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(PairsMain.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "RelativeFrequenciesByPairs");

		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.setMapperClass(PairsMapper.class);
		job.setReducerClass(PairsReducer.class);

		//set keyin, keyout classes for mapper
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// set keyin, keyout classes for job output
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.info("Starting RelativeFrequenciesByPairs");
		int res = ToolRunner.run(new PairsMain(), args);
		System.exit(res);

	}

	
}
