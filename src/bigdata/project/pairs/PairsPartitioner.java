/**
 * 
 */
package bigdata.project.pairs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * @author baopham
 *
 */
public class PairsPartitioner extends Partitioner<Pair, IntWritable> {
	
	private static Logger LOG = Logger.getLogger(PairsPartitioner.class);
	
	@Override
	public int getPartition(Pair key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		LOG.info("Term 1 hash code" + key.term1.hashCode());
		LOG.info("Term 1 hash code % numPartitions" + (key.term1.hashCode() % numPartitions));
		return key.term1.hashCode() % numPartitions;
	}

}
