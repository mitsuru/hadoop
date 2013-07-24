package com.tetsuyaodaka.hadoop.math.matrix;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 *　 CorrelationCoefficientクラス
 *
 * 　相関行列を計算する
 * 
 */
public class CorrelationCoefficient {

	/*
	 *　全データを読み込んで、変量のインデックスをキーとして、Textで書き出す。
	 *
	 */
    public static class MapAll extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        	String strArr[] = value.toString().split("\t");
        	int i= Integer.parseInt(strArr[0]);
//        	String v= strArr[1];

            int m = 0;
            // retrieve from configuration
            int IB 	= Integer.parseInt(context.getConfiguration().get("IB"));
            if(i%IB == 0){
            	m = i/IB; 
            }else{
            	m = i/IB + 1;             	
            }
            context.write(new IntWritable(m), value);
        }
    }

    /*
	 *  対角行列の要素を読み込んで、マークした上でreduceに渡す
	 */
    public static class MapDiag extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] strArr = line.split("\t");
            int i = Integer.parseInt(strArr[0]); // number of column
            value = new Text(line + " diag");

            int m = 0;
            // retrieve from configuration
            int IB 	= Integer.parseInt(context.getConfiguration().get("IB"));
            if(i%IB == 0){
            	m = i/IB; 
            }else{
            	m = i/IB + 1;             	
            }
            context.write(new IntWritable(m), value);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, DoubleWritable>{
		@Override
    	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	Map<Integer,Double> diagMap = new HashMap<Integer,Double>();
        	double diag = 0;
        	int i = 0;
        	List<String> lines = new ArrayList<String>();
        	
        	for(Text value: values){
            	String line = value.toString();
            	if(line.indexOf("diag")!=-1){
                    String[] keyArr = line.split("\t");
                    String[] strArr = keyArr[1].split(" ");
                    diag = Double.parseDouble(strArr[0]);
                    diagMap.put(Integer.parseInt(keyArr[0]), diag);
            	} else {
            		lines.add(line);
            	}
            }
        	
        	for(i=0;i<lines.size();i++){
               	String keyArr[] = lines.get(i).split("\t");
//        		System.out.println(lines.get(i));
        		int numRow = Integer.parseInt(keyArr[0]);
        		diag = diagMap.get(numRow);
               	String strArr[] = keyArr[1].split(" ");
            	for(int j=0;j<strArr.length;j++){
                	double var= Double.parseDouble(strArr[j]);	// number of column
                	var *= diag;
                    BigDecimal bd = new BigDecimal(var);
        			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 
                    context.write(new Text(numRow + " " + (j+1)), new DoubleWritable(r.doubleValue()));
            	}
        	}
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
    	conf.set("I", args[3]); // Num of Row (=Columns)
    	conf.set("IB", args[4]); // RowBlock Size of Matrix
        
    	Job job = new Job(conf, "CalculateCC");

    	job.setJarByClass(CorrelationCoefficient.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Mapperごとに読み込むファイルを変える。
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapAll.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapDiag.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
