package bigdata.project.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>{
	
	private static final String NUMBER_PATTERN = "[0-9]+";
	
	String term1 = "";

	String term2 = "";

	public Pair() {
		super();
	}
	
	public Pair(String term1, String term2) {
		this.term1 = term1;
		this.term2 = term2;
	}

	public void setTerm1(String term1) {
		this.term1 = term1;
	}

	public void setTerm2(String term2) {
		this.term2 = term2;
	}

	public String getTerm1() {
		return this.term1;
	}

	public String getTerm2() {
		return this.term2;
	}

	public String toString() {
		return String.format("(%s, %s)", term1, term2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(term1);
		out.writeUTF(term2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		term1 = in.readUTF();
		term2 = in.readUTF();
	}

	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		if (this.isNumber() && o.isNumber()) {
			int val1 = Integer.parseInt(this.term1);
			int val2 = Integer.parseInt(this.term2);
			
			int val3 = Integer.parseInt(o.term1);
			int val4 = Integer.parseInt(o.term2);
			
			if (val1 == val3) {
				return val2 - val4;
			}
			else {
				return val1 - val3;
			}
		}
		else if (this.term2.equals("*")) {
			if (isNumber(this.term1) && isNumber(o.term1)) {
				int val1 = Integer.parseInt(this.term1);
				int val2 = Integer.parseInt(o.term1);
				if (val1 == val2) {
					return -1;
				}
				else {
					return val1 - val2;
				}
			}
		}
		else if (o.term2.equals("*")) {
			if (isNumber(this.term1) && isNumber(o.term1)) {
				int val1 = Integer.parseInt(this.term1);
				int val2 = Integer.parseInt(o.term1);
				if (val1 == val2) {
					return 1;
				}
				else {
					return val1 - val2;
				}
			}
		}
		int compare = this.term1.compareTo(o.term1);
		if (compare != 0) {
			return compare;
		}
		else {
			return this.term2.compareTo(o.term2);
		}
	}
	
	@Override
	public int hashCode() {
		return this.term1.hashCode() * 31 + this.term2.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Pair) {
			Pair p = (Pair) o;
			return this.term1.equals(p.term1) && this.term2.equals(p.term2);
		}
		return false;
	}

	public boolean isNumber() {
		if (this.term1.matches(NUMBER_PATTERN) && this.term2.matches(NUMBER_PATTERN)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean isNumber(String text) {
		return text.matches(NUMBER_PATTERN);
	}
}
