/**
 * 
 */
package bigdata.project.pairs;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * @author baopham
 *
 */
public class PairsMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
	private static final Logger LOG = Logger.getLogger(PairsMapper.class);
	private Hashtable<Pair, Integer> pairMap = new Hashtable<Pair, Integer>();
	private int one = 1;

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		LOG.info("Starting mapping ");
		if (lineText != null) {
			String[] listTerm = lineText.toString().split("\\s+");
			if (listTerm != null) {
				for (int i = 0; i < listTerm.length - 1; i++) {
					String currentTerm = listTerm[i];
					Pair totalPair = new Pair(currentTerm, "*");
					for (int j = i + 1; j < listTerm.length; j++) {
						if (currentTerm.equals(listTerm[j]))
							break;
						Pair pair = new Pair(currentTerm, listTerm[j]);
						if (pairMap.containsKey(pair)) {
							LOG.info("Pair " + pair + " has already existed");
							int counter = pairMap.get(pair);
							counter++;
							pairMap.put(pair, counter);
						} else {
							pairMap.put(pair, one);
						}

						if (pairMap.containsKey(totalPair)) {
							int counter = pairMap.get(totalPair);
							counter++;
							pairMap.put(totalPair, counter);
						} else {
							pairMap.put(totalPair, one);
						}

					}
				}
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		Enumeration<Pair> enumerator = pairMap.keys();
		while (enumerator.hasMoreElements()) {
			Pair p = enumerator.nextElement();
			LOG.info("<Pair, value> = (" + p.getTerm1() + ", " + p.getTerm2() + "), " + pairMap.get(p));
			context.write(p, new IntWritable(pairMap.get(p)));
		}
	}
}