package bigdata.project.stripes;

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

public class StripesReducer
		extends Reducer<Text, MapWritable, Text, Text> {

	private static final Logger LOG = Logger.getLogger(StripesReducer.class);

	@Override
	public void reduce(Text term, Iterable<MapWritable> stripesList, Context context)
			throws IOException, InterruptedException {
		MapWritable listTermNeighbor = new MapWritable();
		Iterator<MapWritable> listStripes = stripesList.iterator();
		double stripeTotal = 0.0;
		while (listStripes.hasNext()) {
			MapWritable stripe = listStripes.next();

			for (Entry<Writable, Writable> entry : stripe.entrySet()) {
				Text curNeighbor = (Text)entry.getKey();
				if (listTermNeighbor.containsKey(curNeighbor)) {				
					int val1 = ((IntWritable)entry.getValue()).get();
					double val2 = ((DoubleWritable)listTermNeighbor.get(curNeighbor)).get();
					stripeTotal += val1;
					double val = val1 + val2;
					listTermNeighbor.put(curNeighbor, new DoubleWritable(val));
				} else {
					int curVal = ((IntWritable)entry.getValue()).get();
					listTermNeighbor.put(curNeighbor, new DoubleWritable(curVal));
					stripeTotal += curVal;
				}
			}
		}

		for (Entry<Writable,Writable> entry : listTermNeighbor.entrySet()) {
			double curVal = ((DoubleWritable)entry.getValue()).get();
			double frequencies = curVal/stripeTotal;
			LOG.info("CurVal/StripeTotal = " + curVal + "/" + stripeTotal + " => Frequency = " + frequencies);
			frequencies = Double.parseDouble(Utilities.formatDouble(frequencies));
			entry.setValue(new DoubleWritable(frequencies));
		}

		LOG.info("<Term, listTerm> = (" + term + ", " + Utilities.mapWritableToText(listTermNeighbor) + ")");
		context.write(term, Utilities.mapWritableToText(listTermNeighbor));
	}
	

}
