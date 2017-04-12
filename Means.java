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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * Meansクラス
 * 
 * 観測値行列の各変量（列）について、標本平均を計算する。
 * 
 */
public class Means {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
           	String strArr[] = value.toString().split("\t");
        	String keyArr[] = strArr[0].split(" ");
        	int var= Integer.parseInt(keyArr[0]);	// number of column

            context.write(new IntWritable(var), value);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, DoubleWritable>{
    	@Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    		double sum = 0;
            int num = 0;
            for(Text value: values){
               	String strArr[] = value.toString().split("\t");
            	sum += Double.parseDouble(strArr[1]);
                num += 1;
            }
            if(num != 0) sum = sum/num;

            BigDecimal bd = new BigDecimal(sum);
			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 

            context.write(key, new DoubleWritable(r.doubleValue()));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(new Configuration(), "Means");
        job.setJarByClass(Means.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
