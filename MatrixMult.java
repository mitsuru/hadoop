import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *　 Matrix Multiplication on Hadoop Map Reduce
 *
 *   author : tetsuya.odaka@gmail.com
 *   tested on Hadoop1.2
 *   
 *   Split the Large Scale Matrix to SubMatrices.
 *   Split size (Number Of Rows or Columns) can be specified by arguments.
 *   This should be decided according to your resources.
 *   Partitioner and Conditioner are not implemented here.
 *   Can calculate real numbers (format double) and be expected.
 *   
 *   This program is distributed under ASF2.0 LICENSE.
 * 
 */
public class MatrixMult {

	private static int I; // Num of Row of MatrixA
	private static int K; // Num of Row of MatrixB'

	private static int IB; // RowBlock Size of MatrixA
	private static int KB; // RowBlock Size of MatrixB

	private static int M; // Num of RowBlock of MatrixA
	private static int N; // Num of RowBlock of MatrixB
	
	/*
	 *  IndexPair Class
	 *
	 *　reduce用のキーを、MatrixAの行ブロック番号、MatrixBの列ブロック番号、要素番号にする。
	 *　customized key for reduce function consists of row BlockNum of MatrixA, MatrixB, and number of elements.
	 *
	 */
	public static class IndexPair implements WritableComparable<MatrixMult.IndexPair> {
		public int index1;
		public int index2;
		
		public IndexPair() {
		}
		
		public IndexPair(int index1, int index2) {
			this.index1 = index1;
			this.index2 = index2;
		}

		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
		}
		
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
	
		}

		public int compareTo(MatrixMult.IndexPair o) {
			if (this.index1 < o.index1) {
				return -1;
			} else if (this.index1 > o.index1) {
				return +1;
			}
			if (this.index2 < o.index2) {
				return -1;
			} else if (this.index2 > o.index2) {
				return +1;
			}
			return 0;
		}
	}
	
	/*
	 *  MapA Class
	 *
	 *　Matrix Aのデータを読み込んで、行をブロックに分解する。
	 *  read MatrixA and decompose it to blocks
	 *
	 */
    public static class MapA extends Mapper<LongWritable, Text, MatrixMult.IndexPair, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException{

        	String strArr[] = value.toString().split(",");
        	int i= Integer.parseInt(strArr[0]);
        	String v= strArr[1];

            int m = 0;
            if(i%IB == 0){
            	m = i/IB; 
            }else{
            	m = i/IB + 1;             	
            }
            for(int j=1;j<(N+1);j++){
            	context.write(new MatrixMult.IndexPair(m,j), new Text("0"+","+i+","+v));
            }
        }
    }

	/*
	 * MapB Class
	 * 
	 *　Matrix B'のデータを読み込んで、行をブロックに分解する。
	 *  read MatrixB and decompose it to blocks
	 *
	 */
    public static class MapB extends Mapper<LongWritable, Text, MatrixMult.IndexPair, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException{

        	String strArr[] = value.toString().split(",");
        	int k= Integer.parseInt(strArr[0]);
        	String v= strArr[1];
            int n = 0;
            if(k%KB == 0){
            	n = k/KB; 
            }else{
            	n = k/KB + 1;             	
            }
            for(int j=1;j<(M+1);j++){
            	context.write(new MatrixMult.IndexPair(j,n), new Text("1"+","+k+","+v));
            }
        }
    }

	/*
	 * Reduce Class
	 * 
	 */
    public static class Reduce extends Reducer<MatrixMult.IndexPair, Text, Text, DoubleWritable>{
    	@SuppressWarnings("unused")
		@Override
        protected void reduce(MatrixMult.IndexPair key, Iterable<Text> values, Context context) 
        		throws IOException, InterruptedException{

    		List<RowContents> aList = new ArrayList<RowContents>();
        	List<RowContents> bList = new ArrayList<RowContents>();
        	Map<String,List<RowContents>> cMap = new HashMap<String,List<RowContents>>();
        	cMap.put("A",aList);
        	cMap.put("B",bList);

    		for(Text value: values){
            	String strVal = value.toString();
        		
            	String mtx;
            	String sRow;
            	String[] strArray = strVal.split(",");
        		if(Integer.parseInt(strArray[0])==0) {
        			mtx = "A";
        		}else{
        			mtx = "B";
        		}
            	
    			sRow = strArray[1]+","+strArray[2];

            	cMap.get(mtx).add(new RowContents(sRow));
            }
            	
    		for(RowContents ra : cMap.get("A")){
        		for(RowContents rb : cMap.get("B")){
        			int indexA = ra.index;
        			int indexB = rb.index;
        			double sum = 0;
        			for(int i=0;i<ra.lstRow.size();i++){
        				sum += ra.lstRow.get(i)*rb.lstRow.get(i);
        			}
                    context.write(new Text(indexA + " " + indexB+ " "), new DoubleWritable(sum));
        		}
    		}
    			
        }
    	
    	public class RowContents {
    		public String 	strRow;
    		public int 		index;			// means row index
    		public List<Double> 	lstRow;	// list of elements of row.

    		public RowContents() {
    		}

    		public RowContents(String strRow) {
    			this.strRow = strRow;
    			this.lstRow = new ArrayList<Double>();
    			this.calculate();
    		}
    		
    		public void calculate(){
    			String[] strArr = this.strRow.split(",");
    			this.index = Integer.parseInt(strArr[0]);
    			String[] aArr 	= strArr[1].split(" ");
                for(int i=0; i<aArr.length; i++){
                     	this.lstRow.add(Double.parseDouble(aArr[i]));
                }
                return;
    		}
    		
    	}
    	
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Date startProc = new Date(System.currentTimeMillis());
    	System.out.println("process started at " + startProc);
    	
    	Job job = new Job(new Configuration(), "MatrixMultiplication");
        job.setJarByClass(MatrixMult.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(MatrixMult.IndexPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Mapperごとに読み込むファイルを変える。
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapA.class); // matrixA
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapB.class); // matrixB
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // output path

        I = Integer.parseInt(args[3]); // Num of Row of MatrixA
		K = Integer.parseInt(args[4]); // Num of Row of MatrixB'

		IB = Integer.parseInt(args[5]); // RowBlock Size of MatrixA
		KB = Integer.parseInt(args[6]); // RowBlock Size of MatrixB'
		
		if(I%IB == 0){
			M = I/IB;
		}else{
			M = I/IB+1;
		}
		if(K%KB == 0){
			N = K/KB;
		}else{
			N = K/KB+1;
		}
		System.out.println("num of MatrixA RowBlock(M) is "+M);
		System.out.println("num of MatrixB RowBlock(N) is "+N);

		boolean success = job.waitForCompletion(true);

    	Date endProc = new Date(System.currentTimeMillis());
    	System.out.println("process ended at " + endProc);

    	System.out.println(success);
    }
}
