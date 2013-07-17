package com.tetsuyaodaka.hadoop.math.matrix;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * InvertSDクラス
 * 
 * 標準偏差の逆数を計算する。
 * 
 */
public class InvertSD {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
           	String strArr[] = value.toString().split("\t");
        	int var= Integer.parseInt(strArr[0]);
        	double inv = 1/Double.parseDouble(strArr[1]);

            BigDecimal bd = new BigDecimal(inv);
			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 

            context.write(new IntWritable(var), new DoubleWritable(r.doubleValue()));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(new Configuration(), "InvertSD");
        job.setJarByClass(InvertSD.class);

        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
