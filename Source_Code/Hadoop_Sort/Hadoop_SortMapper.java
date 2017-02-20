package Hadoop_Sort.Hadoop_Sort;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class Hadoop_SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String val = value.toString();
        if (val != null && !val.isEmpty() && val.length() >= 5) {
        	String key_toSort = val.substring(0, 10);
        	String value_ofKey = val.substring(10, val.length());
            context.write(new LongWritable(Long.parseLong(key_toSort)),
                    new Text(value_ofKey));
        }
    }
 
}


