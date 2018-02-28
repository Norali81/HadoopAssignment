import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * This class implements a combined key consisting of the page count (number) 
 * and the acronym for the language (language). 
 * 
 * Source adapted from 
 * White, T. (2012). Hadoop: The Definitive Guide (Third Edition edition). Beijing: O’Reilly Media.
 * Page 262
 */

public class CountLangPair implements WritableComparable<CountLangPair>{
	private IntWritable number;
	private Text language;
	
	
	// constructors 
	
	// constructor 1, no arguments
	public CountLangPair() {
		set(new IntWritable(), new Text());
	}
	
	// constructor 2, String and Int argument
	public CountLangPair(int number, String language){
		set(new IntWritable(number), new Text(language));
	}
	
	// constructor 3, IntWritable and Text arguments
	public CountLangPair(IntWritable number, Text language) {
		set(number, language);
	}

	// read fields method
	@Override
	public void readFields(DataInput in) throws IOException {
		number.readFields(in);
		language.readFields(in);
	}

	// write the output
	@Override
	public void write(DataOutput out) throws IOException {
		number.write(out);
		language.write(out);
	}

	//compareTo method helps sorting first by language and then by number (pagecount)
	@Override
	public int compareTo(CountLangPair pair) {
		// compare one language value with another
		int cmp= language.compareTo(pair.language);
		if (cmp !=0) {
			return cmp;
		}
		// if they are the same, compare by number
		return number.compareTo(pair.number);
	}
	
	// setter
	public void set(IntWritable number, Text language) {
		this.number= number;
		this.language = language;
	}
	
	// getter number
	public IntWritable getNumber() {
		return number;
	}
	// getter language
	public Text getLanguage() {
		return language;
	}
	
	// other mandatory functions
	@Override
	public int hashCode() {
	return number.hashCode() * 163 + language.hashCode();
	}
	@Override
	public boolean equals(Object o) {
	if (o instanceof CountLangPair) {
	CountLangPair pair = (CountLangPair) o;
	return number.equals(pair.number) && language.equals(pair.language);
	}
	return false;
	}
	@Override
	public String toString() {
	return number + "\t" + language;
	}

}