package scadir.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

// mapreduce version of get K random initial clusters from the dataset

public class kmeans_init {
//	public  long feature_num = 0;
	
	/*init random clusters from extracted features folder to initial_cluster_path
	 * */
	public static void random_init(String input_dataset, String initial_cluster_path, long cluster_num, Configuration conf) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		// get the feature_count of features
		long feature_num = getFeatureCount( input_dataset, conf);
		// get the random initial features, if init fail, do it over again
		boolean success = false;
		while( success == false){
			
			HadoopUtil.delete(initial_cluster_path);
			success = getRandomInitClusters(input_dataset, initial_cluster_path, cluster_num, feature_num);
			System.out.println(success);
		}
		
	}
	
	//mapreduce job of getting cluster_num globally random features from the input_dataset features
	private static boolean getRandomInitClusters(String input_dataset, String initial_cluster_path, long cluster_num, long feature_num) 
			throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		boolean success = false;
		
		Configuration conf=new Configuration();
		//pass the parameters
		conf.set("feature_num", "" + feature_num);
		conf.set("cluster_num", "" + cluster_num);
		conf.set("initial_cluster_path", initial_cluster_path);
		Job job = new Job(conf);
		
		
		job.setJobName("Kmeans random init");
		job.setJarByClass(kmeans_init.class);
		job.setMapperClass(kmeans_init.RandomEmitMappper.class);
		job.setReducerClass(kmeans_init.getResultReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);


		FileInputFormat.setInputPaths(job, new Path(input_dataset));
		
		FileOutputFormat.setOutputPath(job, new Path(initial_cluster_path));
		
		//Important
		job.setNumReduceTasks(1);

		job.waitForCompletion(true);
		
		
		//check if the outputpath contains "_true" file
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(initial_cluster_path));
		for(FileStatus filestatus : status){
			if(filestatus.getPath().getName().endsWith("_true")){
				success = true;
//				HadoopUtil.delete(str);
				break;
			}
		}
		
		return success;
		
	}
	
	// iterate the features dataset and emit features with probability of cluster_num/feature_num
	public static class RandomEmitMappper extends  Mapper<Text,  VectorWritable, Text, VectorWritable> {
		static int feature_num = 0;
		static int cluster_num = 0;
		static Random rand = null;
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
			feature_num = Integer.parseInt(conf.get("feature_num"));
			cluster_num = Integer.parseInt(conf.get("cluster_num"));
			rand = new Random();
		}
		
		//emitting the same key to avoid intermediate sorting
		//need to emit more than cluster_num features in order to get sufficient candidates
		@Override
		public void map( Text key,VectorWritable value, Context context) throws IOException, InterruptedException {
			if(rand.nextInt(feature_num) < cluster_num * 1.5){
				key.set("0");
				context.write(key, value);
			}
		}
		
	}
	// iterate the features dataset and emit features with probability of cluster_num/feature_num
	//need to set this reducer to be just one!!!
	public static class getResultReducer extends  Reducer<Text,  VectorWritable, IntWritable, VectorWritable> {
		static int feature_num = 0;
		static int cluster_num = 0;
		static String outputpath = null;
		static Random rand = null;
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
			feature_num = Integer.parseInt(conf.get("feature_num"));
			cluster_num = Integer.parseInt(conf.get("cluster_num"));
			outputpath = conf.get("initial_cluster_path");
			System.out.println(outputpath + " " + cluster_num + " " + feature_num);
			rand = new Random();
		}
		
		//emitting the same key to avoid intermediate sorting
		//need to emit more than cluster_num features in order to get sufficient candidates
		// write a "_true" file to the directory to indicate that the operation has accomplished
		@Override
		public void reduce(Text key, Iterable<VectorWritable> values, Context context) 
				throws IOException, InterruptedException {
			int num_processed = 0;
			for(VectorWritable vw : values){
				context.write(new IntWritable(num_processed), vw);
				num_processed ++;
				if(num_processed >= cluster_num){
					break;
				}
			}
			if(num_processed == cluster_num){
				Configuration conf = context.getConfiguration();
				FileSystem fs =FileSystem.get(conf);
				FSDataOutputStream writer = fs.create(new Path(outputpath, "_true"));
//				StringBuilder sb=new StringBuilder();
//				sb.append("" + feature_count);
//				byte[] byt=sb.toString().getBytes();
//				writer.write(byt);
				writer.close();
			}
		}
		
	}

	public static long getFeatureCount(String input_dataset, Configuration conf) throws IOException {
		// TODO Auto-generated method stub
		long num = 0;
		FileSystem fs = FileSystem.get(conf);
		String feature_count_path = new Path( new Path(input_dataset).getParent(), "feature_count").toString();
		String all_files[] = HadoopUtil.getListOfFiles(feature_count_path);
		for(String file : all_files){
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
			String line;
			line=br.readLine();
			while (line != null){
//				System.out.println("count for a file  " + line);
				num += Long.parseLong(line);
			    // be sure to read the next line otherwise you'll get an infinite loop
				line = br.readLine();
			}
			br.close();
		}
		System.out.println("feature count: " + num);
		return num ;
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Path p = new Path("test//test////test//tes//");
		System.out.println(p.toString());
		random_init("test_fe_seq2seq_100images/data/features", "initial_cluster_path", 1000, new Configuration());
	}

}
