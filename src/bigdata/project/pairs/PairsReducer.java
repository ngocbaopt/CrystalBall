/**
 * 
 */
package bigdata.project.pairs;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * @author baopham
 *
 */
public class PairsReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	private static final Logger LOG = Logger.getLogger(PairsReducer.class);
	private int marginal = 0;
	String currentTerm = "";

	@Override
	public void reduce(Pair key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		double relativeFrequency = 0.0;
		if (currentTerm.equals("")) {
			currentTerm = key.term1;
		} else if (!currentTerm.equals(key.term1)) {
			marginal = 0;
			currentTerm = key.term1;
		}
		LOG.info("Current Pair" + key);
		Iterator<IntWritable> iterator = counts.iterator();
		while (iterator.hasNext()) {
			int val = iterator.next().get();
			LOG.info("Value " + val);
			if (key.term2.equals("*")) {
				marginal += val;
			} else {
				sum += val;
			}
		}

		LOG.info("Current pair (" + key);
		LOG.info("Marginal " + marginal);
		LOG.info("Sum " + sum);
		if (!key.term2.equals("*")) {
			relativeFrequency = (double) sum / marginal;
			relativeFrequency = Double.parseDouble(formatDouble(relativeFrequency));
			LOG.info("Frequency " + relativeFrequency);
			context.write(key, new DoubleWritable(relativeFrequency));
		}

	}

	public String formatDouble(double num) {
		DecimalFormat formatter = new DecimalFormat("#0.00");
		return formatter.format(num);
	}

}