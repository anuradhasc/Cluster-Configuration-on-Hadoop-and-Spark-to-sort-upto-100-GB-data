package Hadoop_Sort.Hadoop_Sort;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class Hadoop_SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
	 
@Override
protected void reduce(LongWritable key, Iterable<Text> value, Context context)
    throws IOException, InterruptedException {
for(Text val : value) {
    context.write(new Text(val + "," + key), null);
}
}

}


