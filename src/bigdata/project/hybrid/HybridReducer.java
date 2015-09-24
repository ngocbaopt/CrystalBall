package bigdata.project.hybrid;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private static final Logger LOG = Logger.getLogger(HybridReducer.class);
	private int marginal = 0;
	private MapWritable listNeighbor = new MapWritable();
	String currentTerm = null;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		if (currentTerm == null)
			currentTerm = key.getTerm1();
		else if (!currentTerm.equals(key.getTerm1())) {
			for (Entry<Writable, Writable> entry : listNeighbor.entrySet()) {
				double value = ((DoubleWritable) entry.getValue()).get();
				double frequency = value / marginal;
				frequency = Double.parseDouble(Utilities.formatDouble(frequency));
				LOG.info("Pair = " + key + " value/marginal = " + value + "/" + marginal + " => frequency = " + frequency);
				entry.setValue(new DoubleWritable(frequency));
			}
			context.write(new Text(currentTerm), Utilities.mapWritableToText(listNeighbor));
			// reset for the new term
			marginal = 0;
			currentTerm = key.getTerm1();
			listNeighbor = new MapWritable();
		}
		Iterator<IntWritable> listCount = counts.iterator();
		Text curNeighbor = new Text(key.term2);
		while (listCount.hasNext()) {
			int count = listCount.next().get();
			marginal += count;
			if (listNeighbor.containsKey(curNeighbor)) {
				double curVal = ((DoubleWritable)listNeighbor.get(curNeighbor)).get();
				double newVal = curVal + count;
				listNeighbor.put(curNeighbor, new DoubleWritable(newVal));
			} else {
				listNeighbor.put(curNeighbor, new DoubleWritable(count));
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// NOTHING
		LOG.info("Closing at Reducer");
		for (Entry<Writable, Writable> entry : listNeighbor.entrySet()) {
			double value = ((DoubleWritable) entry.getValue()).get();
			double frequency = value / marginal;
			frequency = Double.parseDouble(Utilities.formatDouble(frequency));
			LOG.info("Pair = " + entry.getKey() + " value/marginal = " + value + "/" + marginal + " => frequency = " + frequency);
			entry.setValue(new DoubleWritable(frequency));
		}
		LOG.info("<Term, Stripes> = (" + currentTerm + ", " + Utilities.mapWritableToText(listNeighbor) + ")");
		context.write(new Text(currentTerm), Utilities.mapWritableToText(listNeighbor));
	}

}
