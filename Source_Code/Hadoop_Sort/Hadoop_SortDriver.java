package Hadoop_Sort.Hadoop_Sort;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Hadoop_SortDriver {
	
	public static void main(String[] args) {
        try {
        	String InputFile = "Input.txt" ;
        	String OutputFile = "Output.txt";
        	String partitionFile = "Temp.txt";
        	// giving num of tasks 40 here for 10 GB.
        	int numReduceTasks = 40;
            runJob(InputFile, OutputFile, partitionFile, numReduceTasks);
        } catch (Exception ex) {
        System.out.println(ex);        }
    }
	
	public static void runJob(String in, String ou,
            String pL, int nRT) throws IOException,
            URISyntaxException, ClassNotFoundException, InterruptedException {
		Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        
        Configuration con = new Configuration();
        Job job = new Job(con, "BigFileSorter");
        job.setJarByClass(Hadoop_SortDriver.class);
        job.setMapperClass(Hadoop_SortMapper.class);
        job.setReducerClass(Hadoop_SortReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(numReduceTasks);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Hadoop_SortKeyComparator.class);
 
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output
                + ".file.sorted." + sdf.format(d)));
        job.setPartitionerClass(TotalOrderPartitioner.class);
 
        Path inpDir = new Path(pL;
        Path pFile = new Path(inpDir, "partition");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
                pFile);
 
        int maxSplits = numReduceTasks - 1;
        if (0 >= maxSplits)
            maxSplits = Integer.MAX_VALUE;
 
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(10.0,
                numReduceTasks, maxSplits);
        InputSampler.writePartitionFile(job, sampler);
 
        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException ex) {
            System.out.println(ex);
        }
    }
	 
}
