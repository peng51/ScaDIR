package scadir.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 * a wrapper class around Mahout kmeans to avoid parameter setting
 */
public class KMeansWrapper {
	
	private static double delta = 0.001; //0.0001; // need to vary this to explore
	
	private static int maxIterations = 20;//for testing
	private static int clusterInitType = 0;
	
	public static int dmType = 1;
	private static final CosineDistanceMeasure cdm = new CosineDistanceMeasure();
	private static final EuclideanDistanceMeasure edm = new EuclideanDistanceMeasure();
	private static DistanceMeasure dm = cdm;
	
	public static void init(double delta, int dmType, int clusterInitType){
		KMeansWrapper.delta = delta;
		KMeansWrapper.dmType = dmType;
		if(dmType == 0) dm = cdm; else dm = edm;
		KMeansWrapper.clusterInitType = clusterInitType;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, boolean is_input_directory) 
			throws InstantiationException, IllegalAccessException, IOException, ClassNotFoundException, InterruptedException {
		
		org.apache.hadoop.conf.Configuration conf = new Configuration();
		Path input_path = new Path(input);
		Path clusters_path = new Path(clusters + "/part-r-00000");
		Path output_path = new Path(output);
		HadoopUtil.delete(output);
		double clusterClassificationThreshold = 0; 
			
		if(clusterInitType == 0)clusters_init_serial(input, clusters_path, k, conf, is_input_directory);
		else clusters_init_random(input, clusters_path, k, conf, is_input_directory);
		//System.out.println("initial " + k + "  clusters written to file: " + clusters);			
		KMeansDriver.run(conf, input_path, clusters_path, output_path, 
					dm, delta, maxIterations, true, clusterClassificationThreshold, false);		
	}
	
	public static void clusters_init_serial(String input, Path clusters_path, int k, Configuration conf, boolean is_input_directory) 
			throws IOException, InstantiationException, IllegalAccessException{
		//read first K points from input folder as initial K clusters
		Path initial_path = null;
		if(is_input_directory==true){//is directory
			String[] input_all_files=HadoopUtil.getListOfFiles(input);
			System.out.println("\n!!!Generate initial cls from path "+input_all_files[0]+"\n");
			initial_path=new Path(input_all_files[0]);
		}
		else initial_path=new Path(input);
				
		// read k points as initial k clusters
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), initial_path, conf);
		WritableComparable key = (WritableComparable)reader.getKeyClass().newInstance();
		VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
		for (int i = 0; i < k; i++){
			reader.next(key, value);	 
			Vector vec = value.get();
			Kluster cluster = new Kluster(vec, i, dm);
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		reader.close(); 
		writer.close();
	}
	
	public static void clusters_init_random(String input, Path clusters_path, int k, Configuration conf, boolean is_input_directory) 
			throws IOException, InstantiationException, IllegalAccessException{
		
		Path initial_path = null;
		if(is_input_directory == true){//is directory
			String[] input_all_files = HadoopUtil.getListOfFiles(input);
			System.out.println("\n!!!Generate random initial cls from path " + input_all_files[0] + "\n");
			initial_path = new Path(input_all_files[0]);
		}
		else initial_path = new Path(input);			
				
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), initial_path, conf);
		WritableComparable in_key = (WritableComparable)reader.getKeyClass().newInstance();
		VectorWritable in_value = (VectorWritable) reader.getValueClass().newInstance();
		//Get random K cluster, store in hashmap
		HashMap<Integer, Kluster> Random_K_cluster = new HashMap<Integer, Kluster>();
		int cluster_id = 0;
		Random rand = new Random();
		while(reader.next(in_key, in_value)){
			if(Random_K_cluster.size() < k){ //fill hashmap with k clusters
				Vector vec = in_value.get();
				Kluster cluster = new Kluster(vec, cluster_id, dm);
				Random_K_cluster.put(cluster_id, cluster);
				cluster_id = cluster_id + 1;
			}
			else {//randomly replace some of the clusters.
				int flag = rand.nextInt(2);
				if(flag % 2 == 0){ //even, replace an existing random kluster.
					int index = rand.nextInt(k);// the cluster to replace 
					Vector vec = in_value.get();
					Kluster cluster = new Kluster(vec, index, dm);
					Random_K_cluster.put(index, cluster);
				}
			}
		}
		
		reader.close(); 
		if(Random_K_cluster.size() != k)
			throw new IOException("kmeans init error, wrong number of initial clusters");
				
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
		SortedSet<Integer> keys = new TreeSet<Integer>(Random_K_cluster.keySet());
		for (Integer out_key : keys) { 
			Kluster out_value = Random_K_cluster.get(out_key);
			writer.append(new Text(out_value.getIdentifier()), out_value);
		}
		writer.close();
	}
	
}
