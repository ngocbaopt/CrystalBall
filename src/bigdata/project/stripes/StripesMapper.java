package bigdata.project.stripes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class StripesMapper extends Mapper<LongWritable, Text, CrystalBallText, MapWritable> {
	
	private static final Logger LOG = Logger.getLogger(StripesMapper.class);

	private IntWritable one = new IntWritable(1);

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		LOG.info("Starting mapping");
		if (lineText != null) {
			String[] listTerm = lineText.toString().split("\\s+");
			if (listTerm != null) {
				for (int i = 0; i < listTerm.length - 1; i++) {
					String currentTerm = listTerm[i];
					MapWritable stripes = new MapWritable();
					for (int j = i+1; j < listTerm.length; j++) {
						if (currentTerm.equals(listTerm[j]))
							break;
						Text curNeighbor = new Text(listTerm[j]);
						if (stripes.containsKey(curNeighbor)) {
							int counter = ((IntWritable)stripes.get(curNeighbor)).get();
							counter++;
							stripes.put(curNeighbor, new IntWritable(counter));
						} else {
							stripes.put(curNeighbor, one);
						}
					}
					LOG.info("<Term, stripes> = (" + currentTerm + ", " + Utilities.mapWritableToText(stripes) + ")");
					context.write(new CrystalBallText(currentTerm), stripes);
				}
			}
		}
	}
}
