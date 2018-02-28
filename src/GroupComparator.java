import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * This is the grouping comparator for the secondary sort
 */
public class GroupComparator extends WritableComparator {
	//constructor
	protected GroupComparator() {
		super(CountLangPair.class, true);
	}
	
	//compare method
	public int compare(WritableComparable w1, WritableComparable w2){
		CountLangPair pair1 = (CountLangPair) w1;
		CountLangPair pair2 = (CountLangPair) w2;
		return pair1.compareTo(pair2);
	}

}
