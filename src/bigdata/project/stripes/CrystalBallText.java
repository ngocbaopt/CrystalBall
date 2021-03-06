/**
 * 
 */
package bigdata.project.stripes;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * @author baopham
 *
 */
public class CrystalBallText extends Text {

	private static final Logger LOG = Logger.getLogger(CrystalBallText.class);

	private static final String NUMBER_PATTER = "[0-9]+";

	public CrystalBallText() {
		super();
	}

	public CrystalBallText(String text) {
		super(text);
	}

	@Override
	public int compareTo(BinaryComparable text) {
		String text1 = this.toString();
		String text2 = text.toString();

		LOG.info("comparing " + text1 + " with " + text2);

		if (isNumber(text1) && isNumber(text2)) {
			int val1 = Integer.parseInt(text1);
			int val2 = Integer.parseInt(text2);
			return val1 - val2;
		} else {
			return super.compareTo(text);
		}
	}

	public boolean isNumber(String text) {
		return text.toString().matches(NUMBER_PATTER);
	}
}
