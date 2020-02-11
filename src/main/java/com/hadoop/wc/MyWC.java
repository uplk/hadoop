package com.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Properties;

public class MyWC {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //Test conflict: Project Manager
        // to solve win-user doesnt have authority
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "hadoop");

        // to solve Failed to connect to :master:50010
        Configuration conf = new Configuration(true);
        conf.set("dfs.client.use.datanode.hostname", "true");

        Job job = Job.getInstance(conf);
        
        job.setJarByClass(MyWC.class);

        job.setJobName("test_win_hadoop");

        Path input = new Path("/1.txt");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/hadoop_test/wc/");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);
    }
}
