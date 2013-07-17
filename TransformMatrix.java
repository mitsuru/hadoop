package com.tetsuyaodaka.hadoop.math.matrix;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 *　 Matrix Multiplication on Hadoop Map Reduce(Step1)
 *
 *   author : tetsuya.odaka@gmail.com
 *   tested on Hadoop1.2 
 *   
 *   Split the Large Scale Matrix to SubMatrices.
 *   Split size (Number Of Rows or Columns) can be specified by arguments.
 *   
 *   This should be decided according to your resources.
 *   Partitioner and Conditioner are not implemented here.
 *   Can calculate real numbers (format double) and be expected.
 *   
 *   This program is distributed under ASF2.0 LICENSE.
 * 
 */
public class TransformMatrix {
	
	/*
	 *  Map Class
	 *
	 *  read MatrixA and decompose its elements to blocks
	 *
	 */
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException{

        	String strArr[] = value.toString().split("\t");
        	String keyArr[] = strArr[0].split(" ");

            // retrieve from configuration
            boolean tr 	= Boolean.parseBoolean(context.getConfiguration().get("transpose"));	// row block size

            if(tr){
            	int n= Integer.parseInt(keyArr[0]);	// number of column
            	context.write(new IntWritable(n), value);
            }else{
            	int n= Integer.parseInt(keyArr[1]);	// number of row
            	context.write(new IntWritable(n), value);
            }
        }
    }
    
	/*
	 * Reduce Class
	 * 
	 */
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{

		@Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
        		throws IOException, InterruptedException{

    		HashMap<Integer, Double> aMap = new HashMap<Integer, Double>();

    		// retrieve from configuration
            boolean tr 	= Boolean.parseBoolean(context.getConfiguration().get("transpose"));	// row block size
            
        	for(Text value: values){
            	String strVal = value.toString();
            	String strArr[] = strVal.split("\t");
            	String keyArr[] = strArr[0].split(" ");
            	
            	Double val = Double.parseDouble(strArr[1]);
                BigDecimal bd = new BigDecimal(val);
    			BigDecimal r = bd.setScale(2, BigDecimal.ROUND_HALF_UP); 

    			if(tr) {
            		aMap.put(Integer.parseInt(keyArr[1]),r.doubleValue());

            	}else{
            		aMap.put(Integer.parseInt(keyArr[0]),r.doubleValue());
        		}
        	}

        	Set<Integer> setA = aMap.keySet();
        	Set<Integer> sortedSetA = new TreeSet<Integer>(setA);
        	StringBuffer sb = new StringBuffer();
        	for(int indexA : sortedSetA){
        		if(indexA > 1) sb.append(" ");
        		sb.append(aMap.get(indexA));
        	}
            context.write(key, new Text(sb.toString()));
       }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Date startProc = new Date(System.currentTimeMillis());
    	System.out.println("process started at " + startProc);
    	
    	Configuration conf = new Configuration();
        if(args[2].equals("yes")){
        	conf.set("transpose", "true"); // transpose
        }else{
        	conf.set("transpose", "false"); // 
        }

        Job job = new Job(conf, "MatrixMultiplication");
        job.setJarByClass(TransformMatrix.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Mapperごとに読み込むファイルを変える。
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map.class); // matrixA
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        boolean success = job.waitForCompletion(true);

    	Date endProc = new Date(System.currentTimeMillis());
    	System.out.println("process ended at " + endProc);

    	System.out.println(success);
    }
}
