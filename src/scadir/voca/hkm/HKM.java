package scadir.voca.hkm;

import scadir.util.HadoopUtil;
import scadir.util.KMeansWrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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


public class HKM {
	
	private  static int cluster_capacity = 1000; // in number of containers, change this 
	private  static int num_jobs_botlevelclustering = 100;
	
	private static int topK = 0;
	private static int botK = 0;
	
	//example args: data/cluster/fs.seq data/cluster/level  10 10
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException{
		//run(args, 1);
		//setParallelDegree("test_fe_seq2seq_100images/cluster/mid/", 10, 1);
	}
	
	public static String run(String[] args, int botlvlcluster_type) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		long ts0 = 0, ts1 = 0, ts2 = 0, ts3 = 0;
		try {
			ts0 = new Date().getTime();
			// parse parameters
			String data = args[0];
			String prefix = args[1];
			String top = prefix + "/top";
			String mid = prefix + "/mid";
			String bot = prefix + "/bot";
			topK = Integer.parseInt(args[2]);
			botK = Integer.parseInt(args[3]);
			String clusters = prefix + "/clusters";
			
			// execute top-down clustering pipeline
			HadoopUtil.delete(prefix);
			// top-level clustering
			topLevelProcess(data, top + "/cls", top, topK);
			ClusterDump.getClusterResults(top, clusters, 0, 0);
			
			ts1 = new Date().getTime();
			// medium processing between top-level and bottom-level clustering
			midLevelProcess(top, mid);
			
			//bottom level clustering
			//setParallelDegree(mid, topK, botlvlcluster_type);
			ts2 = new Date().getTime();
			if(botlvlcluster_type == 0) botLevelProcess_Serial(mid, bot, topK, botK);
			else if(botlvlcluster_type == 1) botLevelProcess_MRJob(mid, bot, topK, botK);
			else if(botlvlcluster_type == 2) botLevelProcess_MultiThread(mid, bot, topK, botK);
			
			// write all the bottom-level clusters to a text file
			merge(bot, prefix);
			// write bottom-level clusters into separeate files
			ClusterDump.getMultipleClusterResults(bot, clusters, 1);
			
			ts3 = new Date().getTime();
			log("top-down clustering pipeline ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "top-level clustering time = " +  (double)(ts1 - ts0) / (60 * 1000) + "\n" +
		"mid-level processing time = " + (double)(ts2 - ts1) / (60 * 1000) + "\n" +
		"bot-level clustering time = " + (double)(ts3 - ts2) / (60 * 1000) + "\n";
	}
	
	

	//set the number of botlevel clustering job run in parallel
/*	public static void setParallelDegree(String input_mid_folders, int topK, int botlevelclusteringtype) throws IOException{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] folders = getFolders(input_mid_folders);
		double containers_needed = 0;
		for(String folder : folders){//set the number of botlevel clustering job run in parallel
			ContentSummary cs =fs.getContentSummary(new Path(folder));
			double input_size= (double) cs.getLength();//cs.getSpaceConsumed();
			System.out.println(input_size);
			double input_mb =  Math.ceil(input_size/(1024*1024));
			System.out.println("Input: " + folder + "  size in MB : " + input_mb);
			//assuming each map container can process 128 mb data and each reducer can process 64 mb, and 50% jobs in reduce phase
			//should be tuned depending on cluster's setting and percentage of jobs in reduce phase should also be tuned
			containers_needed = containers_needed + Math.ceil(input_mb/128) + 0.5 * Math.ceil(input_mb/64); 
		}
		
		System.out.println("Total containers needed to run all jobs in parallel: " + containers_needed);
		double avg_containers_per_job = Math.ceil( containers_needed / topK );
		if(botlevelclusteringtype == 1){
			//num_jobs_botlevelclustering + num_jobs_botlevelclustering*avg_containers_per_job < cluster_capacity -1
			num_jobs_botlevelclustering = (int) ((cluster_capacity - 1) / (1 + avg_containers_per_job));
		}
		else{
			num_jobs_botlevelclustering = (int) (cluster_capacity / avg_containers_per_job);
		}
		
		System.out.println("\n\nAverage containers per job : " +  avg_containers_per_job
				+ "\nCluster's Capacity is " + cluster_capacity
				+ "\nExpected maximum number of jobs parallelly run : " + num_jobs_botlevelclustering);
		
		//using fixed number of setting
		num_jobs_botlevelclustering = 50;
		System.out.println("\nset number of concurrent jobs = " + num_jobs_botlevelclustering);
	}
	*/
	
	public static void topLevelProcess(String input, String cls, String top, int topK)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		KMeansWrapper.kmeans(input, cls, top, topK, true);
	}
	
	public static void midLevelProcess(String top, String mid) throws IOException, InterruptedException {
		ClusterPP.run_clusterpp(top + "/clusteredPoints", mid);
	}
	
	public static void botLevelProcess_Serial(String mid, String bot, int topK, int botK) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		
		String[] folders = getFolders(mid);
		for(int i = 0; i < folders.length; i++){
			// run mahout k-means clustering
			String input = folders[i] + "/part-m-0";
			String clusters = bot + "/" + i + "/cls";
			String output = bot + "/" + i;
			int k = botK;
			KMeansWrapper.kmeans(input,clusters, output, k, false);
			log("botlevel mahout kmeans clustering " + i + "> ends");
		}
	}
	

	public static void botLevelProcess_MRJob(String mid, String bot, int topK, int botK) throws IOException {
		
		String[] folders = getFolders(mid);
		String output_folder = bot + "/temp";
		HadoopUtil.delete(output_folder);
		List<String> params = new ArrayList<String>();
		
		for(int i = 0; i < num_jobs_botlevelclustering && i < folders.length; i++){
			params.clear();
			for(int j = 0; i + j < folders.length; j = j + num_jobs_botlevelclustering){
				int index = i + j;
				String input = folders[index] + "/part-m-0";
				
				String clusters = bot + "/" + index + "/cls";
				String output = bot + "/" + index;
				int k = botK;
				String param = input + " " + clusters + " " + output + " " + k;
				params.add(param);
			}
			writeBotLevelParameters(""+i,params, output_folder + "/" + i + ".txt");			
		}
		
		runBotLevelClustering.run(output_folder, bot + "/whatever");
		HadoopUtil.delete(bot + "/whatever");
		HadoopUtil.delete(bot + "/temp");
	}
	
	public static void writeBotLevelParameters(String key, List<String> values, String filename) throws IOException{
			Path output_path = new Path(filename);	 
			Configuration conf = new Configuration();
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, output_path, Text.class, Text.class);
			for(String value:values){
				writer.append(new Text(key), new Text(value));
			}
			writer.close();
	}
	
	public static void botLevelProcess_MultiThread(String mid, String bot, int topK, int botK) throws IOException, InterruptedException {
		String[] folders = getFolders(mid);
		KmeansThread[] threads = new KmeansThread[num_jobs_botlevelclustering];
		
		//init all KmeansThread
		for(int i = 0; i < threads.length; i++){
			threads[i] = new KmeansThread();
		}
		
		// add parameters to kmeans threands
		for(int i = 0; i < folders.length; i++){
			String input = folders[i] + "/part-m-0";
			String clusters = bot + "/" + i + "/cls";
			String output = bot + "/" + i;
			int k = botK;
			threads[i % num_jobs_botlevelclustering].addParams(new Kmeans_Params(i, input, clusters, output, k));
		}
		
		//start all kmeans threads
		for(KmeansThread kt : threads){
			kt.start();
		}
		
		//wait for all threads to complete
		for(int i = 0; i < num_jobs_botlevelclustering; i++){
			threads[i].join();
		}
		
	}
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{	
		// copy and merge files
		String temp = dst + "/dumptemp";
		//gather all the clustered points in result directory and merge them.
		//those files should be in test/cluster/bot/i/cluster-j-final/* and not start with '_'
		ArrayList<String> inputs = new ArrayList<String>();
		String[] bot_folders = HadoopUtil.getListOfFolders(src);
		for (String bot_folder : bot_folders){
			String[] bot_i_folders = HadoopUtil.getListOfFolders(bot_folder);
			if(bot_i_folders.length == 1 && bot_i_folders[0].endsWith("final") == false)
				bot_i_folders = HadoopUtil.getListOfFolders(bot_i_folders[0]);
			for(String folder : bot_i_folders)
				if(folder.endsWith("final")) inputs.add(folder);
		}
		ArrayList<String> inputs_files = new ArrayList<String>();
		for(String input : inputs){
			String[] files = HadoopUtil.getListOfFiles(input);
			for(String file:files)
				if(file.startsWith("_") == false) inputs_files.add(file);
		}
		for(String file : inputs_files){
			System.out.println("files to merge: " + file);
		}
		
		String[] inputs_clusterdump = new String[inputs_files.size()];
		inputs_clusterdump = inputs_files.toArray(inputs_clusterdump);
		ClusterDump.run_clusterdump(inputs_clusterdump, temp);
		HadoopUtil.copyMerge(temp, dst + "/clusters.txt");
		
		//TODO need to reassign cluster ID to be consistent with readcluster in frequency job
/*		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(dst + "/clusters_temp.txt"))));
		 BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(dst + "/clusters.txt"),true)));
		String inline = null;
		int clusterId = 0;
		while((inline = br.readLine()) != null){
			String cluster = inline.split("\t")[1];
			bw.write("" + clusterId + "\t" + cluster + "\n");
			clusterId ++;
		}
		br.close();
		bw.flush();bw.close();
*/
		
		
	}
	
	public static String[] getFolders(String mid){
		String[] tmp_folders=HadoopUtil.getListOfFolders(mid);
		System.out.println("In path:"+ mid);			
		//threads[i].start();
		for(String str:tmp_folders)
			System.out.println("folder: "+str);
		if(tmp_folders == null || tmp_folders.length != topK){
			System.out.println("Error: number of folders in dir:  " + mid + "   does not equal to " + topK + ", please check!!!");
			return tmp_folders;
		}
		else return tmp_folders;
	}
	
	@SuppressWarnings("deprecation")
	public static void log(String msg){
		Date now = new Date();
		System.out.println((now.getYear() - 100) + "/" + (now.getMonth() + 1) + "/" + now.getDate() + " " +  
				now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds() + ": " + msg);
	}
	
}

class runBotLevelClustering{
	public static void run(String input, String whatever_output){
		//HadoopUtil.delete(output);
		System.out.println("botlevel clustering using MR job parallelism!!!!!!!");

		JobConf conf = new JobConf(runBotLevelClustering.class);
		conf.setJobName("botlevelclustering");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ClusterWritable.class);

		conf.setMapperClass(runBotLevelClustering.KmeansMap.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
	    
	    //no reduce step operation is needed
	    conf.setNumReduceTasks(0);
	    
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(whatever_output));
		

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("botlevel clustering is done");
	}
	
	public static class KmeansMap extends MapReduceBase implements Mapper<Text, Text, IntWritable,  ClusterWritable> {
		@Override
		public void map(Text key, Text value, OutputCollector<IntWritable,  ClusterWritable> output, Reporter reporter) 
				throws IOException {
			String args=value.toString();
			//String input, String clusters, String output, int k, double cd, int x
			String[] splits=args.split(" ");
			
			try {
				KMeansWrapper.kmeans(splits[0], splits[1], splits[2], Integer.parseInt(splits[3]), false);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			HKM.log("bottom-level clustering " + key.toString() + " ends");
		}
	}
}

class KmeansThread extends Thread
{
	List<Kmeans_Params> params =new ArrayList<Kmeans_Params>();
	
	public void addParams(Kmeans_Params params){
		this.params.add(params);
	}

   @Override
   public void run()
   {
    	  for(Kmeans_Params param : params){
	    		try {
	    			KMeansWrapper.kmeans(param.input, param.clusters, param.output, param.k,false);
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		HKM.log("job no. " + param.id + " ends");
    	  }
   }
}

class Kmeans_Params{
	public int id;
	public String input;
	public String clusters;
	public String output;
	public int k;
	public Kmeans_Params(int id, String input, String clusters, String output, int k){
		  this.id = id;
	      this.input = input;
	      this.clusters = clusters;
	      this.output = output;
	      this.k = k;
	}
}