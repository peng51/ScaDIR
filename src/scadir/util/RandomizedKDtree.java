/*
 * build a randomized kd tree upon and
 * 
 * */
package scadir.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class RandomizedKDtree{

	
	public static final int partition_size = 5;// the maximum number of points a leaf node can hold
	public Node root =null;
	
	/*
	 * build a randomized kd tree with random num_d dimensions on vector array varray, won't change the varray 
	 * split axis will be chosen from top random num_d dimensions that have the largest variance.s (currently it's not random though)
	 * each level of split dimension will be chosen randomly from those top dimensions
	 * */
	public Node buildTree( int[] dimensions, float[][] varray){
		
		int[] points = new int[varray.length];
		for(int i = 0; i < varray.length; i++)
			points[i] = i;
		
		long startTime1 = System.nanoTime();
		root = buildKDTree(dimensions, 0,  varray, points, 0,  varray.length - 1);

		points = null;
//to release mem
		points = null;
		long endTime1 = System.nanoTime();
		System.out.println("building tree took " + ((double)( endTime1 - startTime1 )/(1000 * 1000 * 1000)) + "secs");
		return root;
	}

	
	
	/*
	 * recursively build kd tree on  sub array of varray, specified by points
	 * each node will have no more than partition_size number of points
	 * 
	 * @param dim : the dimensions to split on
	 * @param level: the recusive level, used to decide which dim to split on.
	 * @param varry: the array to build kdtree upon
	 * @param points: list of integers indicate the vectors in varray that we need to build kdtree on
	 * 
	 * internal nodes should not contain actual points though
	 * 
	 * */
	private Node buildKDTree(int[] dim, int level, float[][] varray, int[] points, int startIndex, int endIndex) {
		// TODO Auto-generated method stub
		
		
		//no data points for this node, should be null and return;
		if(startIndex > endIndex){
			return null;
		}
		
		Node n = new Node();
		
		// if poins.length <= partitions_size, reach leaf node
		if(endIndex - startIndex + 1 <= partition_size){
			n.points = new int [endIndex - startIndex + 1];
			for(int i = startIndex; i <= endIndex; i ++){
				n.points[i - startIndex] = points[i];
			}
//			n.points[0] = startIndex; n.points[1] = endIndex;
			n.left = null;
			n.right = null;
			n.split_axis = -1;
			n.split_value = Integer.MIN_VALUE;
		}
		
		//else need to split further into smaller partitions -- internal node
		else{
			
			//choose split axis randomly here
			int s_a = new Random().nextInt(dim.length);
			n.split_axis = dim[s_a];
//			System.out.println("split axis " + s_a);

			//according to the split axis, sort points from startIndex to endIndex according to the values of axis they point to, 
			//return the split value
			n.split_value = sortPoints(n.split_axis, varray, points, startIndex, endIndex);
			int left_start = startIndex;
			int left_end = startIndex + (endIndex - startIndex)/2;
			int right_start = left_end +1;
			int right_end = endIndex;
			
			int[] dim_left = KDTreeForest.getTopDimensionsWithLargestVariance(dim.length, points, left_start, left_end,  varray);
			int[] dim_right = KDTreeForest.getTopDimensionsWithLargestVariance(dim.length, points, right_start, right_end, varray);
			//recursive build the left tree and right tree
			n.left = buildKDTree(dim_left, level + 1, varray, points, left_start, left_end);
// release if possible(internal node will have a pointer to the points' array and will thus keep it
			n.right = buildKDTree(dim_right, level + 1, varray, points, right_start, right_end);
			n.points = null;
			
			dim_left = null;
			dim_right = null;
			
		}
		points = null;
		return n;
	}

	//sort the points from startIndex to endIndex of varry of the specified split_axis
	// start and end index inclusive!!!!
	private float sortPoints(int split_axis, float[][] varray, int[] points, int startIndex, int endIndex) {
		// TODO Auto-generated method stub
		float split_value = 0;
		Index_Value[] list = new Index_Value[endIndex - startIndex + 1];
		
		for(int i = startIndex; i <= endIndex; i ++){
			list[i - startIndex] = new Index_Value(points[i], varray[points[i]][split_axis]);
		}
		
		Arrays.sort(list, new Comparator<Index_Value>(){
			@Override
			public int compare(Index_Value arg0, Index_Value arg1) {
				// TODO Auto-generated method stub
				if( arg0.value - arg1.value > 0)
					return 1;
				else if(arg0.value - arg1.value <0)
					return -1;
				else return 0;
			}
			});
		
		if(list.length % 2 == 0)
			split_value = (list[list.length / 2].value + list[list.length / 2 - 1].value) / 2;
		else{
			split_value = list[list.length / 2].value;
		}
		//get the sorted points 
		for(int i = startIndex; i <= endIndex; i++){
			points[i] = list[i - startIndex].index;
		}
//release mem.
		list = null;
		return split_value;
	}
	
	//get the Euclidean distance of two vectors
	public static float getDistance(float[] v1, float[] v2) throws Exception {
		// TODO Auto-generated method stub
		if(v1 == null || v2 == null){
			throw new Exception("null node distance error");
			
		}
		else if (v1.length != v2.length){
			throw new Exception("vector not of the same size distance error");
		}
		//get eclidean distance
		float sum = 0;
		for(int i = 0; i < v1.length; i ++){
			sum = sum + (v1[i] - v2[i]) * (v1[i] - v2[i]);
		}
		return (float) Math.sqrt(sum);
	}

	public static void main(String args[]) throws Exception{
		
	}
}
//used for neareast neigbor search -- used as "global variables" to store the currently nearest neighbor found
class NNS{
	public  Float minDistance = Float.MAX_VALUE;
	public  int nnId = -1;
	
	//test to check the number of comparisons 
	public int comparisons = 0;
}


class Node {
	
public	int split_axis =-1;
public	float split_value = Float.MIN_VALUE;
public	Node left = null;
public	Node right = null;
public	int[] points = null;
	
}
class Index_Value{
	public int index;
	public float value;
	public Index_Value(int i, float v){
		this.index = i;
		this.value = v;
	}
}
