package Hadoop_Sort.Hadoop_Sort;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class Hadoop_SortKeyComparator extends WritableComparator  {
	protected Hadoop_SortKeyComparator() {
        super(LongWritable.class, true);
    }
 
    // Compares in the ascending order of the keys.
  
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LongWritable o1 = (LongWritable) a;
        LongWritable o2 = (LongWritable) b;
        if(o1.get() > o2.get()) {
            return 1;
        }else if(o1.get() < o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
}
