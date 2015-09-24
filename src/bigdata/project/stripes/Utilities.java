/**
 * 
 */
package bigdata.project.stripes;

import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author baopham
 *
 */
public class Utilities {

	public static Text mapWritableToText(MapWritable map) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		int i = 0;
		for (Entry<Writable, Writable> entry : map.entrySet()) {
			i++;
			sb.append("(");
			sb.append(entry.getKey());
			sb.append(", ");
			sb.append(entry.getValue());
			sb.append(")");
			if (i != map.size())
				sb.append(", ");
		}
		sb.append("]");
		return new Text(sb.toString());
	}
	
	public static String formatDouble(double num) {
		DecimalFormat formatter = new DecimalFormat("#0.00");
		return formatter.format(num);
	}
}
