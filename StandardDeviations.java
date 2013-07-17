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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *　 StandardDeviationsクラス
 *
 * （すでに計算された）平均と観測値をつかって、（不偏）標準偏差を算出する。
 * 
 */
public class StandardDeviations {

	/*
	 *　全データを読み込んで、変量のインデックスをキーとして、Textで書き出す。
	 *
	 */
    public static class MapAll extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
           	String strArr[] = value.toString().split("\t");
        	String keyArr[] = strArr[0].split(" ");
        	int var= Integer.parseInt(keyArr[0]);	// number of column

            context.write(new IntWritable(var), value);
        }
    }

    /*
	 *　変量ごとの算術平均の計算結果を読んで、変量のインデックスをキーとして、Textで書き出す。
	 *　この際、平均値の後ろにmeanとつけて、reduceで読んだときのマークとする。
	 *
	 */
    public static class MapMean extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] strArr = line.split("\t");
            value = new Text(strArr[1]+" mean");
    		context.write(new IntWritable(Integer.parseInt(strArr[0])), value);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, DoubleWritable>{
    	@Override
    	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	double sum = 0;
        	double num = 0;
        	double mean = 0;
            for(Text value: values){
            	if(this.isMean(value.toString())){
            		mean = this.getMean(value.toString());
            	} else {
                   	String strArr[] = value.toString().split("\t");
            		sum += Double.parseDouble(strArr[1]) * Double.parseDouble(strArr[1]);
            		num++;
            	}
            }

            double ss = (sum - num*mean*mean)/(num-1);
            double s  = Math.sqrt(ss);
            BigDecimal bd = new BigDecimal(s);
			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 
            context.write(key, new DoubleWritable(r.doubleValue()));
        }
    	
    	private boolean isMean(String line){
    		if(line.indexOf("mean")==-1) return false;
    		return true;
    	}
    	
    	private double getMean(String line){
            String[] strArr = line.split(" ");
    		return Double.parseDouble(strArr[0]);
    	}
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(new Configuration(), "StandardDeviations");
        job.setJarByClass(StandardDeviations.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Mapperごとに読み込むファイルを変える。
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapAll.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapMean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
