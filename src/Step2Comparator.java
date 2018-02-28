import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Comparator to control descending sorting order
 */
public class Step2Comparator extends WritableComparator {
	
	// constructor
	public Step2Comparator(){
		super(IntWritable.class,true);
	}
	
	// reverse sorting order
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// reverse sign of the comparison result of the superclass
        return -1 * super.compare(b1, s1, l1, b2, s2, l2);
    }		

}
