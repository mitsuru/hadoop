package com.tetsuyaodaka.hadoop.math.matrix;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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
 *　 MatrixSubMeansクラス
 *
 * （すでに計算された）変量ごとの平均を、観測値行列から引く。
 * 
 */
public class MatrixSubMeans {

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
            int oKey = Integer.parseInt(strArr[0]); // number of column
            value = new Text(strArr[1]+" mean");
    		context.write(new IntWritable(oKey), value);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, DoubleWritable>{
    	@Override
    	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	double mean = 0;
        	List<String> list = new ArrayList<String>();
            for(Text value: values){
            	String line = value.toString();
            	if(line.indexOf("mean")!=-1){
                    String[] strArr = line.split(" ");
                    mean = Double.parseDouble(strArr[0]);
            	} else {
            		list.add(line);
            	}
            }
            
            
            for(int i=0;i<list.size();i++){
                String l=list.get(i);
               	String strArr[] = l.split("\t");
            	double var= Double.parseDouble(strArr[1]);	// number of column
            	var -= mean;
            	var /= Math.sqrt(list.size()-1);
                BigDecimal bd = new BigDecimal(var);
    			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 
                context.write(new Text(strArr[0]), new DoubleWritable(r.doubleValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(new Configuration(), "SubtractMeans");
        job.setJarByClass(MatrixSubMeans.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Mapperごとに読み込むファイルを変える。
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapAll.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapMean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
