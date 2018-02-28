import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Key comparator for the secondary sort
 */
public class KeyComparator extends WritableComparator{
	
	// constructor
	public KeyComparator(){
		super(CountLangPair.class,true);
	}
	
	// compare method
	public int compare(WritableComparable w1, WritableComparable w2){
		CountLangPair pair1 = (CountLangPair) w1;
		CountLangPair pair2 = (CountLangPair) w2;
		
		// Sort first by language and then by number
		int cmp = pair1.getLanguage().compareTo(pair2.getLanguage());
		// if result of comparing is null, then compare by number
		if (cmp !=0) {
			return cmp;
		}
		return pair1.getNumber().compareTo(pair2.getNumber());
	}
}

