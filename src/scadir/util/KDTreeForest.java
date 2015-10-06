package scadir.util;

import scadir.feat.FeatureExtraction;
import scadir.feat.SIFT;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// forest of KDTrees for approximate nearest neighbor search
public class KDTreeForest {
	public static final int num_trees = 8;
	public static final int num_dimensions = 5; 
	public static double max_comparison = 0.05;// suggestion: should set this to about 5% to 15 % of of the total nodes???
	
	public Node[] roots = null;
	
	
	
	//test main
	public static void main(String args[]) throws Exception{
		
		
	//	get_features();
		
		//read features from file
		ArrayList<float[]> q_vectors = new ArrayList<float[]>();
		ArrayList<float[]> varray = new ArrayList<float[]>();
/*		BufferedReader br = new BufferedReader(new FileReader(new File("data/features_images.txt")));
		String inline = null;
		while((inline = br.readLine()) != null){
			String[] splits = inline.split(" ");
			float[] vector = new float[128];
			for(int i = 0; i < 128; i ++ ){
				vector[i] = Float.parseFloat(splits[i]);
			}
			
			//normalize the vector -- euclidean norm
			float sq_sum = 0;
			for(int i = 0; i < vector.length; i ++){
				sq_sum += vector[i]*vector[i];
			}
			sq_sum = (float) Math.sqrt(sq_sum);
//			System.out.println(sq_sum);
			for(int i = 0; i < vector.length; i ++){
				vector[i] = vector[i] / sq_sum;
			}
			
			//use first 2000 as query, the rest as cluster data to build trees upon
			if(q_vectors.size() < 2000){
				q_vectors.add(vector);
			}
			else{
				varray.add(vector);
			}
			
			
		}
		br.close();
*
*/
		String inline = null;
		BufferedReader br = new BufferedReader(new FileReader(new File("data/ImageNet_test_AKM/clusters.txt")));
		while((inline = br.readLine()) != null){
			String[] splits = inline.split(" ");
			float[] vector = new float[128];
			for(int i = 0; i < 128; i ++ ){
				vector[i] = Float.parseFloat(splits[i]);
			}
			varray.add(vector);
		}
		br.close();
		br = new BufferedReader(new FileReader(new File("data/ImageNet_test_AKM/query.txt")));
		while((inline = br.readLine()) != null){
			String[] splits = inline.split(" ");
			float[] vector = new float[128];
			for(int i = 0; i < 128; i ++ ){
				vector[i] = Float.parseFloat(splits[i]);
			}
			q_vectors.add(vector);
		}
		
		
		
		System.out.println("clusters size:" + varray.size() + "\nqueries size: " + q_vectors.size());
		
		float[][] dataset = new float[varray.size()][128];
		for(int i = 0; i < varray.size(); i ++){
			dataset[i] = varray.get(i);
		}
		
		
		
		KDTreeForest kdtf = new KDTreeForest();//kdtf = kdtreeforest
		kdtf.build_forest(dataset);
		
		double precision = 0;
		double time_kdtf = 0;
		double time_serial = 0;
		
		//test the precision and the avg speed up
		//run all the queries
		System.out.println("query starts, can take a little bit of time...");
		for(float[] q_vector : q_vectors){
			long startTime = System.nanoTime();
			
			//get the approximate nearest neighbor  with BBF method
			int id = kdtf.nns_BBF(dataset, q_vector);
			
			long endTime = System.nanoTime();
			time_kdtf += ((float)( endTime - startTime )/(1000 * 1000 * 1000));

			
			//serial way to find the nearest vector
			long startTime1 = System.nanoTime();
			double min_dist = Double.MAX_VALUE;
			int nnId = -1;
			for(int i = 0; i < dataset.length; i ++){
				double dist = RandomizedKDtree.getDistance(dataset[i], q_vector);
				
				if(dist < min_dist){
					nnId = i;
					min_dist = dist;
				}
			}
			long endTime1 = System.nanoTime();
			
			time_serial += ((double)( endTime1 - startTime1)/(1000 * 1000 * 1000));
			
			
			if (id == nnId){
				precision ++;
			}
			//speedup = speedup + exact_search_time/kdtf_time;
		}
		double speedup =  time_serial / time_kdtf;
		precision = precision / q_vectors.size();
		
		System.out.println("avg precision : " + precision + " avg speedup : " + speedup 
				+ "\t for " + num_trees + " trees  and " + num_dimensions + " dmensions and maximum comparisons : " + max_comparison );
	}
	
	
	/*
	 * build a forest of kd trees
	 * choose num_total_dimensions as the dimensions to split on
	 * build num_trees of trees
	 * 
	 * */
	public void build_forest(float[][]  varray){
		this.roots = new Node[num_trees];
		int[] points = new int[varray.length];
		for(int i = 0; i < points.length; i ++){
			points[i] = i;
		}
		int[] dims = getTopDimensionsWithLargestVariance(num_dimensions, points, varray);
		for(int i = 0; i < num_trees; i ++){
			RandomizedKDtree rt = new RandomizedKDtree();
			roots[i] = rt.buildTree(dims, varray);
		}
	}
	
	
	/* nearest neighbor search -- best bin first
	 * 
	 */
	public int nns_BBF(float[][] varray, float[] q_vector) throws Exception{
		Comparator<Node_p> nc = new NodeComparator();
		PriorityQueue<Node_p> queue = new PriorityQueue<Node_p>(100, nc);
		
		//nn store the nearest neighbor found so far
		NNS nn = new NNS();
		nn.nnId = 0;
		nn.minDistance = RandomizedKDtree.getDistance(varray[nn.nnId], q_vector);
		nn.comparisons = 0;
		
		/*visited information about the nodes
		 * */
		boolean[] visited = new boolean[varray.length];
		for(int i = 0; i < visited.length; i ++){
			visited[i] = false;
		}
		//enqueue roots first with 0 -- highest priority
		for(Node node : roots){
			if(node != null){
				queue.add(new Node_p(node, 0));
			}
		}
		
		// while loop for visiting the nodes util maximum comparison number reached Or  nearest neighbor found.
		while(queue.isEmpty() == false){// && checks < max_nn_checks){
			Node_p np = queue.poll();
			
			//the current found min distance is already smaller that the distance to the nearest hyperplance, terminate search
			if(nn.minDistance <= np.distance){
				return nn.nnId;
			}
			
			// from this node, explore to leaf, and enqueue those unvisited other child
			Node node = np.node;
			while(node != null){
//				
				// if non-leaf node, need to explore further down
				if(node.left != null || node.right != null){
					//need explore left first, enqueue right for later use
					if(q_vector[node.split_axis] <= node.split_value){
						if(node.right != null){
							queue.add(new Node_p(node.right, Math.abs(node.split_value - q_vector[node.split_axis])));
						}
						node = node.left;
					}
					//else need to explore right first, enqueue left
					else{
						if(node.left != null){
							queue.add(new Node_p(node.left, Math.abs(node.split_value - q_vector[node.split_axis])));
						}
						node = node.right;
					}
				}
				//else reached leaf node, explore the leaf node
				else{
					for(int point : node.points){
						if( visited[point] == false){
							float distance = RandomizedKDtree.getDistance(q_vector, varray[point]);
							if(distance < nn.minDistance){
								nn.nnId = point;
								nn.minDistance = distance;
							}
							nn.comparisons ++;
							visited[point] = true;
							// if reached maximum comparisons, need to terminate search
							if (nn.comparisons > max_comparison * varray.length){
								//	System.out.println("number of comparisons : " + nn.comparisons  + ""
								//		+ " wasted calculations " + wasted_calculation);
								return nn.nnId;
							}
						}
					}
					//setting null to exit while loop
					node = null;
				}
			//	System.out.println(node + " " + node.split_axis +node.split_value + node.left + node.right + node.points[0]);				
			}
		}
		
		// System.out.println("number of comparisons : " + nn.comparisons  + " wasted calculations " + wasted_calculation);
		return nn.nnId;
	}

	//get the top num_d dimensions with the largest variance for splitting the space
	//the points will contain the indexes of the valid data points in varray.
	public static int[] getTopDimensionsWithLargestVariance(int num_d, int[] points, float[][] varray) {
		// TODO Auto-generated method stub
		int[] dims = new int[num_d];
		float[] variance_num_d = new float[num_d];
		
		float[] all_variances = new float[varray[0].length];
		
		for(int k = 0; k < num_d; k ++){
			variance_num_d[k] = 0;
		}
		
		for(int i = 0; i < varray[0].length; i ++){
			
			//calc variance for dim = i
			float variance = 0;
			float mean = 0;
			for(int j = 0; j < points.length; j++){
				variance = variance + varray[points[j]][i] * varray[points[j]][i];
				mean = mean + varray[points[j]][i];
			}
			mean = mean / varray.length;
			variance = variance - varray.length * mean * mean;
			
			all_variances[i] = variance;
			//update the dims array
			int k = 0;
			for(k = 0 ; k < dims.length; k ++){
				if(variance > variance_num_d[k])
					break;
			}
			for(int s = dims.length - 1; s > k; s --){
				dims[s] = dims[s - 1];
				variance_num_d[s] = variance_num_d[s - 1];
			}
			if(k < dims.length){
				dims[k] = i;
				variance_num_d[k] = variance;
			}
			
		}
		
		
		return dims;
	}
	
	///test main
	// get sift features from the images and write them to file.
	public static void get_features() throws IOException{
		ArrayList<double[]> varray = new ArrayList<double[]>();
		String[] files = HadoopUtil.getListOfFiles("data/images");
		FileSystem fs = FileSystem.get(new Configuration());
		
		for(String file : files){
			BufferedImage img = ImageIO.read(fs.open(new Path(file)));
			System.out.println("extracting sift features from " + file);
			String[] features = SIFT.getFeatures(img);
			for(String feature : features){
				double[] feature_vector = FeatureExtraction.FEMap.getPoints(feature.split(" "), 128);

					varray.add(feature_vector);
			}
		}
	}


	public static int[] getTopDimensionsWithLargestVariance(int num_d,	int[] points, int start, int end, float[][] varray) {
		// TODO Auto-generated method stub
		int length = end - start +1;
		int[] dims = new int[num_d];
		float[] variance_num_d = new float[num_d];
		
		float[] all_variances = new float[varray[0].length];
		
		for(int k = 0; k < num_d; k ++){
			variance_num_d[k] = 0;
		}
		//for each dimensions, calculate the variances of the points from start to end
		for(int i = 0; i < varray[0].length; i ++){
			
			//calc variance for dim = i
			float variance = 0;
			float mean = 0;
			for(int j = 0; j < length; j++){
				variance = variance + varray[points[start + j]][i] * varray[points[start + j]][i];
				mean = mean + varray[points[start + j]][i];
			}
			mean = mean / length;
			variance = variance - length * mean * mean;
			
			all_variances[i] = variance;
			//update the dims array
			int k = 0;
			for(k = 0 ; k < dims.length; k ++){
				if(variance > variance_num_d[k])
					break;
			}
			for(int s = dims.length - 1; s > k; s --){
				dims[s] = dims[s - 1];
				variance_num_d[s] = variance_num_d[s - 1];
			}
			if(k < dims.length){
				dims[k] = i;
				variance_num_d[k] = variance;
			}
			
		}
		
		
		return dims;
	}

}

//used in priority queue
class Node_p{
	public Node node;
	public float distance;
	public Node_p(Node n, float d){this.node = n; this.distance = d;}
}

class NodeComparator implements Comparator<Node_p>{

	@Override
	public int compare(Node_p arg0, Node_p arg1) {
		// TODO Auto-generated method stub
		if(arg0.distance > arg1.distance)
			return 1;
		else if(arg0.distance < arg1.distance)
			return -1;
		else 
			return 0;
	}
	
}
