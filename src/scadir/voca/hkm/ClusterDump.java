package scadir.voca.hkm;

import scadir.util.HadoopUtil;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;


public class ClusterDump {
	public static void main(String args[]) throws IOException, InterruptedException{
		//run_clusterpp("data/cluster/top/clusteredPoints", "data/cluster/tmpmid/");
		//HadoopUtil.delete("temptemp");
		//TopDownClustering.merge("data/cluster/level/res", "temptemp");
		HadoopUtil.copyMerge("test/cluster/bot/temp/", "test/cluster/clusters.txt");
	}
	
	// multi-thread implementation of getting the clusters from bot level clustering
	public static void getMultipleClusterResults(String input, String outputfolder, int level) throws NumberFormatException, IOException, InterruptedException {
		String folders[] = HadoopUtil.getListOfFolders(input);
		if(folders == null || folders.length == 0){
			throw new FileNotFoundException(input);
		}
		
		int parallel_degree = 10;
		if(parallel_degree > folders.length){
			parallel_degree = folders.length;
		}
		
		ClusterResultThread[] threads = new ClusterResultThread[parallel_degree];
		//init all KmeansThread
		for(int i = 0; i < threads.length; i++){
			threads[i] = new ClusterResultThread();
		}
		// add parameters to  threads
		for(int i = 0; i < folders.length; i++){
			String splits[] = folders[i].split("/");
			String clusterId = splits[splits.length - 1];
			threads[i % parallel_degree].addParams(new ClusterResult_Params(folders[i], outputfolder, level, Integer.parseInt(clusterId)));
		}
		//start all threads
		for(ClusterResultThread ct : threads){
			ct.start();
		}
		//wait for all threads to complete
		for(int i = 0; i < parallel_degree; i++){
			threads[i].join();
		}
	}

	//get the cluster centroids of the output of kmeans clustering and output them to the output folder with level n folder and key.txt
	//for toplevel clustering result : level = 0, clusterID = 0
	public static void getClusterResults(String input, String outputfolder, int level, int clusterId) throws IOException {
		// TODO Auto-generated method stub
		String[] folders = HadoopUtil.getListOfFolders(input);
		String folder = null;
		for(String f:folders){
			if(f.endsWith("-final")){
				folder = f;
			}
		}
		if(folder == null){
			throw new FileNotFoundException(input + "/...final");
		}
		String files[] = HadoopUtil.getListOfFiles(folder);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path output_folder = new Path(outputfolder + "/" + level);
		Path output = new Path(outputfolder + "/" + level + "/" + clusterId + ".txt");
		if(fs.exists(output_folder) == false){
				fs.mkdirs(output_folder);
		}
			
		FSDataOutputStream out = fs.create(output);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		for(String file : files){
			if(file.startsWith("_") == false){
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);
				IntWritable key = new IntWritable();
				ClusterWritable value = new ClusterWritable();
				while(reader.next(key, value)){
					bw.write(value.getValue().getId() + "\t" + value.getValue().getCenter().toString() + "\n");
				}
				reader.close();
			}
		}
		bw.flush();bw.close();
	}
	
	//read in the classified points in multiple locations  and output them to a single file
	public static void run_clusterdump(String[] inputs, String output){
		HadoopUtil.delete(output);
		System.out.println("output folder: " + output);
		JobConf conf = new JobConf(ClusterDump.class);
		conf.setJobName("clusterdump");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(ClusterDump.ClusterDumpMap.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

		for(String input:inputs){
			FileInputFormat.addInputPath(conf, new Path(input));
		}
		
		FileOutputFormat.setOutputPath(conf, new Path(output));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("clusterdump is done");
	}
	
	public static class ClusterDumpMap extends MapReduceBase implements Mapper<IntWritable, ClusterWritable, Text,  Text> {
		@Override
		public void map(IntWritable key, ClusterWritable value, OutputCollector<Text, Text> output, Reporter reporter) 
				throws IOException {
			output.collect( new Text("" + value.getValue().getId()), new Text(value.getValue().getCenter().toString()));
		}
	}
}

class ClusterResultThread extends Thread
{
	List<ClusterResult_Params> params =new ArrayList<ClusterResult_Params>();
	
	public void addParams(ClusterResult_Params params){
		this.params.add(params);
	}

   @Override
   public void run()
   {
    	  for(ClusterResult_Params param : params){
					try {
						ClusterDump.getClusterResults(param.input, param.output, param.level, param.clusterId);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
	    		HKM.log(param.input + " processed!!!");
    	  }
   }
}


class ClusterResult_Params{
	public String input;
	public String output;
	public int level;
	public int clusterId;
	public ClusterResult_Params(String in, String out, int lvl, int id){
		this.input = in;
		this.output = out;
		this.level = lvl;
		this.clusterId = id;
	}
}
