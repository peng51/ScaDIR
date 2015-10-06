package scadir.repr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import scadir.util.HadoopUtil;


public class Histogram {
	//input: clusters.txt, feature texts
	//output: a file containing both the name of file and the cluster id
	private static  boolean Use_Cosine_distance = true;
	public static int featureSize = 128;
	public static int clusterNum = 100;
	public static String clusters = "";
	public static String features = "";
	
	public static void main(String[] args) throws IOException{
		//runJob("test/data/features/", 100, "test/cluster/clusters.txt", "test/temp/freq", "test/data/frequency.txt");
		runJob(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4]);
		//Frequency.FreMap.readClusters(args[0], Integer.parseInt(args[1]));
	}
	
	public static void init(String features, int clusterNum, String clusters){
		Histogram.clusterNum = clusterNum;
		Histogram.clusters = clusters;
		Histogram.features = features;
	}
	
	public static void runJob (String features, int clusterNum, String clusters, String temp, String output){
		init(features, clusterNum, clusters);
		HadoopUtil.delete(temp);
		HadoopUtil.delete(output);
		runFreMR(features, temp);
		HadoopUtil.copyMerge(temp, output);
	}
	
	public static void runFreMR(String infile, String outfile){
		
		JobConf conf = new JobConf(Histogram.class);
		conf.setJobName("Histogram");
		
		// configure parameters
		conf.set("features", features);
		conf.set("clusterNum", new Integer(clusterNum).toString());
		conf.set("clusters", clusters);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(FreMap.class);
		conf.setReducerClass(FreReduce.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
		FileInputFormat.setInputPaths(conf, new Path(infile));
		FileOutputFormat.setOutputPath(conf, new Path(outfile));
		
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class FreMap extends MapReduceBase implements Mapper<Text, VectorWritable, Text, Text> {
		public static int featureSize = 128;
		public static int clusterNum = 100;
		public static String clusters = "";
		public static String features = "";
		public static double[][] cs = null;
		
		@Override
		public void configure(JobConf job) {
		   clusterNum = Integer.parseInt(job.get("clusterNum"));
		   clusters = job.get("clusters");
		   features = job.get("features");
		   //System.out.println(clusters);
		   try {
			cs = readClusters(clusters, clusterNum);
		   } catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   }//read clusters
		}
		
		@Override
		public void map(Text key, VectorWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String file = key.toString();
			Vector vector = value.get();
			double[] feature = new double[featureSize];
			for (int i = 0; i < featureSize; i++)
				feature[i] = vector.get(i);
			int index = findBestCluster(feature, cs);
			output.collect(new Text(file), new Text(1 + "\t" + index));
			//System.out.println(file + " processed");
		}
		
		public static double[][] readClusters(String clusters, int clusterNum) throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(clusters);
			FSDataInputStream input = fs.open(path);
			double[][] cs = new double[clusterNum][featureSize];
			String line;
			//System.out.println(clusters);
			
			int i = 0;
			while((line = input.readLine()) != null){
				//System.out.println("the start of processing one line");
//debug!!!				int i = Integer.parseInt(line.split("\t")[0]);
				//System.out.println("id = " + i);
				String center = line.split("\\}")[0].split("\\{")[1];
				//System.out.println(center);
				String[] array = center.split(",");
				//if normal case, correct format
				if(center.contains(":") == false) {
					for(int j = 0; j < featureSize; j++){
						String[] co = array[j].split(":"); // adding regex patterns to solve the wrong format problem
						cs[i][j] = Double.parseDouble(co[co.length - 1]);
					}
				}
				else { // abnormal case, fill those missing dimensions with zeros
					String[] result_array = new String[featureSize];
					for (int k = 0; k < featureSize; k ++){
						result_array[k] = "0";
					}
					for(String str : array){
						String[] splits = str.split(":");
						int index = Integer.parseInt(splits[0]);
						result_array[index] = splits[1];
					}
					for(int j = 0; j < featureSize; j++){
						cs[i][j] = Double.parseDouble(result_array[j]);
					}
				}
				i ++;
			}
			return cs;
		}
		
		public static int findBestCluster(double[] feature, double[][] clusters){
			if (Use_Cosine_distance == true){
				int index = -1;
				double distance = -1.1;
				//feature = norm(feature);
				for(int i = 0; i < clusters.length; i++){
					double fl = 0;
					double cl = 0;
					double ds = 0;
					for(int j = 0; j < clusters[i].length; j++){
						ds += feature[j] * clusters[i][j];
						fl += feature[j] * feature[j];
						cl += clusters[i][j] * clusters[i][j];
					}
					ds = ds / (Math.sqrt(fl) * Math.sqrt(cl));
					
					if(ds > distance){
						distance = ds;
						index = i;
					}
				}
				
				return index;
			}
			else {
				int index = -1;
				double distance = Double.MAX_VALUE;
				//feature = norm(feature);
				for(int i = 0; i < clusters.length; i++){
					double ds = 0;
					for(int j = 0; j < clusters[i].length; j++){
						ds += (feature[j] - clusters[i][j]) * (feature[j] - clusters[i][j]);
					}
					ds = Math.sqrt(ds);
					
					if(ds < distance){
						distance = ds;
						index = i;
					}
				}
				return index;
			}
		}
		
	}

	public static class FreReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String sum = "";
			int total = 0;
			while (values.hasNext()) {
				String s = values.next().toString();
				if(!s.equals("")){
					if (sum.length() == 0) sum = s.split("\t")[1];
					else sum = sum + " " + s.split("\t")[1];
					total += Integer.parseInt(s.split("\t")[0]);
				}
			}
			//System.out.println(sum);
			output.collect(key, new Text(total + "\t" + sum));
		}
	}
	
}