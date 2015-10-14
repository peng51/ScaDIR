package scadir.multimodal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import scadir.feat.FeatureExtraction;
import scadir.repr.Histogram;
import scadir.solr.Search;
import scadir.util.HadoopUtil;
import scadir.voca.VCDriver;

/*
 * pipeline for mmd dataset
 */
public class Pipeline_mmd {
	static final boolean use_opencv = false;
	
	//type = 0 vw + tw,
	//type = 1 vw only
	//type = 2 tw only
	static int type = 2;
	
	//store the relevant docs for each of the category
	static HashMap<String, Integer> num_per_category = new HashMap<String, Integer>();
	
	//map query image to category
	static HashMap<String, String> query_img_to_category = new HashMap<String, String>();

	//step 1 FE
	//step 2 VW (clustering, frequency)
	//step 3 add TW to VW
	//step 4 index
	//step 5 search and compute mAP
	
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, 
			IOException, InterruptedException, SolrServerException{
		
		//args[0]
		String eval_data_root = args[0]; //root folder of the eval data
		// eval_data_root/ -- mmd-3/
		//                    --folders of categories each containing the map.txt
		//                 -- query-3 
		//                   - queries.txt
		//                   - query images.
		//args[1]
		String images_seqfile = args[1]; //"multimodal_resized/multimodal.seq"; //args[1]; //need to convert all the images in mmd-1 dir to a single seqfile first
		
		//args[2]
		String topk = args[2]; //"100"; //args[2];
		
		//args[3]
		String botk = args[3]; //"100"; //args[3];
		
		//args[4]
		String output = args[4]; //output for clustering etc.


		HadoopUtil.mkdir(output);
		
		//step 1 FE
		String features = output + "/data/features";
//		FeatureExtraction.extractFeatures(images_seqfile, features);

		
		//step 2 VW: clustering and histgram
		String[] arguments_vw = {features, output, "" + topk, "" + botk};
//		String runningtime = VCDriver.run(arguments_vw, 2,1);
		
		//step 3 : get the vw.txt, tw.txt and vw_tw.txt
		// will generate vw.txt, vw_tw.txt, tw.txt in dst/data dir
		String images_root = eval_data_root +"/mmd-3/";
		MultiFreq.run(images_root, output, num_per_category);
		
		//step 4&5 indexing and searching
		String[] arguments = {features, output, topk, botk, eval_data_root};
		run(arguments);
		
		//combined search
		combinedSearch(arguments);
	}

	
	
	//args[0] = input of extracted features
	//args[1] = output root folder
	//args [2] = topk
	//args[3] = botk
	//args[4] = eval_data_root
	// run clustering and frequecy and calculate mAP (step 4 - 5)
	public static void run(String[] args) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IOException, InterruptedException, SolrServerException{
		String dst = args[1];
		String topk = args[2];
		String botk = args[3];
		String eval_data_root = args[4];
		
		String query_folder = eval_data_root + "/query-3";
		type = 0;//use vw + tw
		double mAP = evaluate(query_folder, topk, botk, dst, new HashMap<String, Double>(), new HashMap<String, String[]>());
		
		System.out.println("mAP is : " + mAP);
		
	}

	
	//search with vw and tw seperately, get the rankedlist of each img search results, combine them together then get the 
	//combined search:
	public static void combinedSearch(String args[]) 
			throws IOException, SolrServerException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException{
		//run multifrequency.txt
		String dst = args[1];
		String topk = args[2];
		String botk = args[3];
		String eval_data_root = args[4];
		

		String query_folder = eval_data_root + "/query-3";
		
		//evaluate
		
		//get all the results of tw only
		type = 2;
		HashMap<String, String[]> search_results_tw = new HashMap<String, String[]>();
		HashMap<String, Double> aps_tw = new HashMap<String, Double>();
		double mAP = evaluate(query_folder, topk, botk, dst, aps_tw, search_results_tw);
		System.out.println("mAP is : " + mAP);
		
		//get vw only
		type = 1;
		HashMap<String, String[]> search_results_vw = new HashMap<String, String[]>();
		HashMap<String, Double> aps_vw = new HashMap<String, Double>();
		mAP = evaluate(query_folder, topk, botk, dst, aps_vw, search_results_vw);
		System.out.println("mAP is : " + mAP);
		
		
		double multiplyfactor = 1.0;
		
		while(multiplyfactor <  1.1){
			multiplyfactor = multiplyfactor + 0.1;
			//get the combined results
			SortedSet<String> keys_aps_vw = new TreeSet<String>(aps_vw.keySet());
			//for each image get the combined results and evaluate them
			HashMap<String, Double> aps_combined = new HashMap<String, Double>();
		    for(String key : keys_aps_vw) {//key is the query image here
		    	double ap_tw = aps_tw.get(key);
		    	double ap_vw = aps_vw.get(key);
		    	System.out.println(key + "\t\t" + ap_tw + "\t\t" + ap_vw);
		    	
		    	double lambda  =  (multiplyfactor) * (ap_tw / (ap_tw + ap_vw));
		    	
		    	String[] results_tw = search_results_tw.get(key);
		    	String[] results_vw = search_results_vw.get(key);
		    	String[] combined_results = getCombinedResults(results_tw, results_vw, lambda);
		    	

		    	
		    	double ap = getAP(query_img_to_category.get(key), combined_results);
		    	aps_combined.put(key, ap);
		    	
		    	/*
		    	 * write some rank results for some images
		    	 * 
		    	 */
		    	String prefix = "multimodal_lmm_resized/query-lmm-new/testing/";
		    	String[] strs = {"Pisa1.jpg", "Ben1.jpg", "Colosseum1.jpg", "St1.jpg"};
		    	boolean flag = false;
		    	for(String str : strs){
		    		if(key.equals(prefix + str)){
		    			flag = true;
		    		}
		    	}
		    	if(flag == true){
		    		String[] splits = key.split("/");
		    		String common_prefix = splits[splits.length - 1];
		    		
			    	BufferedWriter bw_tw_rank = new BufferedWriter(new FileWriter(new File(common_prefix + "tw_rank_returned_results.txt")));
			    	bw_tw_rank.write("TW; category:" + query_img_to_category.get(key) + ", AP: " + ap_tw +"\n");
			    	int rank = 1;
			    	for(String str : results_tw){
			    		String splits2[] = str.split("/");
			    		str = splits2[splits2.length - 2] + "/" + splits2[splits2.length - 1];
			    		bw_tw_rank.write(rank + "," + str + "\n");
			    		rank ++;
			    	}
			    	bw_tw_rank.close();
			    	
			    	BufferedWriter bw_vw_rank = new BufferedWriter(new FileWriter(new File(common_prefix + "_vw_rank_returned_results.txt")));
			    	rank = 1;
			    	bw_vw_rank.write("VW; category:" + query_img_to_category.get(key) + ", AP: " + ap_vw +"\n");
			    	for(String str : results_vw){
			    		String splits2[] = str.split("/");
			    		str = splits2[splits2.length - 2] + "/" + splits2[splits2.length - 1];
			    		bw_vw_rank.write(rank + "," + str + "\n");
			    		rank ++;
			    	}
			    	bw_vw_rank.close();
			    	
			    	BufferedWriter bw_combined_rank = new BufferedWriter(new FileWriter(new File(common_prefix + "_combined_rank_returned_results.txt")));
			    	rank = 1;
			    	bw_combined_rank.write("Combined: category:" + query_img_to_category.get(key) + ", AP: " + ap +"\n");
			    	bw_combined_rank.write("lambda: " + (ap_tw / (ap_tw + ap_vw)) + ", d: " + multiplyfactor + "\n");
			    	for(String str : combined_results){
			    		String splits2[] = str.split("/");
			    		str = splits2[splits2.length - 2] + "/" + splits2[splits2.length - 1];
			    		bw_combined_rank.write(rank + "," + str + "\n");
			    		rank ++;
			    	}
			    	bw_combined_rank.close();
		    	}
		    	
		    	/*
		    	 * end writing results to file
		    	 */
		    	
		    }
		    
			//write the mAPs to file
		    //mAPs_mmd_kmeans_5000_0.5*lambda
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("mAPs_" + "lmm_kmeans_10000_" + multiplyfactor + "*lambda.txt")));
			bw.write("Type: combined: using lambda" + "\n");
		    
			SortedSet<String> keys_combined = new TreeSet<String>(aps_combined.keySet());
			mAP = 0;
		    for(String key : keys_combined) {
		    	double value = aps_combined.get(key);
		    	mAP += value;
		    	System.out.println(key + "\t\t" + value);
		        bw.write(key + "\t\t" + value + "\n");
		    }
		    mAP = mAP / aps_combined.size();
		    bw.write("\nthe mAP is: " + mAP);
		    bw.flush();
		    bw.close();
	    
	}
		
	}

	
	//inex, execute all the queries and return the mAP
	private static double evaluate(String query_folder, String topk, String botk, String dst, HashMap<String, Double> aps, HashMap<String, String[]> search_results) 
			throws IOException, SolrServerException{
		
		
		//indexing
		
		String indexfile = "";
		if(type ==0){
			indexfile = dst + "/data/vw_tw.txt";
		}else if(type ==1){
			indexfile = dst + "/data/vw.txt";
		}if(type ==2){
			indexfile = dst + "/data/tw.txt";
		}
		int clusterNum = Integer.parseInt(topk) * Integer.parseInt(botk);
		Search.init(indexfile, clusterNum, dst + "/cluster/clusters.txt", Integer.parseInt(topk), Integer.parseInt(botk));
		int indexed_images = (int) Search.runIndexing(indexfile);
		
		
		
		
		//process each image as query
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path folder_path = new Path(query_folder + "/queries.txt");
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(folder_path)));
		
		//write the vw words of query images to a file query_folder/queries_vw.txt
		Path queries_vw = new Path(query_folder + "/queries_vw.txt");
		BufferedWriter bw_vw = new BufferedWriter(new OutputStreamWriter(fs.create(queries_vw)));
		
		String inline = null;
		//HashMap<String, Double> all_aps = new HashMap<String, Double>();
		while((inline = br.readLine()) != null){
//TODO
			//for mmd dataset
			String[] splits = inline.split("\\s+");
			String category = splits[0].trim(); // category and | or search tw ???
			String search_tw = category;
			int start_index = 1;
			
			//for lmm data set, category and searh tw are different !!!
//			String[] splits = inline.split("---");
//			String category = splits[1].trim();
//			String search_tw = splits[0].trim();
//			splits = splits[2].trim().split("\\s+");
//			int start_index = 0;
			
			
			for(int i = start_index; i < splits.length; i ++){
				
				String query_img = query_folder + "/" + splits[i].trim();
				if(query_img_to_category.containsKey(query_img) == false){
					query_img_to_category.put(query_img, category);
				}
				try{
					double ap = Integer.MIN_VALUE;
				
					//create the query (with only vw)
					String qs_vw = createQuery(query_img);
					bw_vw.write(query_img + "\t" + qs_vw + "\n");
					//get the qs of vw + tx
					String qs = "";
					
					if(type ==0){
						qs = qs_vw + " " + search_tw;
					}else if(type == 1){
						qs = qs_vw;
					}else if(type == 2){
						qs = search_tw;
					}
					
					System.out.println("QueryImage: " + query_img + ", query string: " + qs);
					
					//get the results from solr
					String[] results = Search.query(qs, indexed_images);
					ap = getAP(category,results);
					
					System.out.println("AP for Image : " + query_img + " is: " + ap);
					aps.put(query_img, ap);
					search_results.put(query_img, results);
					
				}catch(java.lang.ArrayIndexOutOfBoundsException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
				}catch(javax.imageio.IIOException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
				}catch(java.io.EOFException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
				}catch(java.lang.NullPointerException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
				}catch(org.apache.solr.client.solrj.SolrServerException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
				}
				
			}
		}
		br.close();
		bw_vw.flush();
		bw_vw.close();
		double mAP = 0;
		for(Double ap : aps.values()){
			mAP += ap;
		}
		mAP = mAP / aps.size();
		System.out.println("mAP is " + mAP + ", out of #of searched images: " + aps.size());
		
		//write the mAPs to file
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("mAPs_" + new Date().getTime() + ".txt")));
		bw.write("Type: " + type + "\n");
	    
		SortedSet<String> keys = new TreeSet<String>(aps.keySet());
		
	    for(String key : keys) {
	    	double value = aps.get(key);
	    	System.out.println(key + "\t\t" + value);
	        bw.write(key + "\t\t" + value + "\n");
	    }
	    bw.write("\nthe mAP is: " + mAP);
	    bw.flush();
	    bw.close();
		
		return mAP;
	}

	private static double getAP(String category, String[] results){

		
		
		double AP = 0;
		double num_relevant_docs = num_per_category.get(category);
		
		int relevant_detected = 0;
		for(int i = 0; i<results.length; i++){
			
			
			int relevant = getRelevance(results[i], category);
			relevant_detected += relevant;
			double precision = ((double) relevant_detected) / (i + 1);
			AP += ( precision * relevant ) / num_relevant_docs;
		}
		System.out.println(relevant_detected + "\t" + num_relevant_docs);
		if(relevant_detected != num_relevant_docs){
			System.out.println("Error found that actual relevant docs and relevant detected  : "
					 + num_relevant_docs + "  " + relevant_detected + " are not equal, please check(retrieve results lengh :" + results.length);
		}
		return AP;
	}
	
	private static int getRelevance(String filename, String category) {
		String[] splits = filename.split("/");
		if(splits[splits.length - 2].equals(category)){
			return 1;
		}
		else{
			return 0;
		}
	}

	static double[][] clusters = null;
	public static String createQuery(String img_path) throws IOException{//transform an image into a Solr document or a field
		
		if(clusters == null){
			clusters = Histogram.FreMap.readClusters(Search.clusters, Search.clusterNum);
		}
		
		int[] marks = new int[Search.clusterNum];
		double [][] all_features = null;
		
		//get features into double array all_features
		if(use_opencv == true){
//			all_features = SIFTExtraction_OpenCV.extractSIFT(img_path);
		}
		//else use lire
		else{
			String features[] = Search.getImageFeatures(img_path);
			all_features = new double[features.length][Search.featureSize];
			for(int i = 0; i < all_features.length; i++){
				String[] args = features[i].split(" ");
				for (int j = 0; j < Search.featureSize; j++){
					all_features[i][j] = Double.parseDouble(args[j + 4]);
				}
			}						
		}
		
		
		for(int i = 0; i < all_features.length; i++){
//			int index = Frequency.FreMap.findBestCluster(feature, clusters);
			int index = Histogram.FreMap.findBestCluster(all_features[i], clusters);
			marks[index]++;
		}
			
		String result = "";
		for(int i = 0; i < Search.clusterNum; i++){
			for(int j = 0; j < marks[i]; j++){
				if(result.length() == 0) result += "vw" + i;
				else result += " vw" + i;
			}	
		}
		//System.out.println("query string: " + result);
		return result;
	}
	



	static String[] getCombinedResults(String[] results_tw, String[] results_vw, double lambda) {
		HashMap<String, Double> combined = new HashMap<String, Double>();
	//	int ext_rank_tw = results_tw.length;
	//	int ext_rank_vw = results_vw.length;
		int max_rank = 0;
		if(results_tw.length > results_vw.length){
			max_rank = results_tw.length;
		}
		else{
			max_rank = results_vw.length;
		}
		
		//for each item in results_tw, get their combined results with results_vw 
		for(int i = 0; i < results_tw.length; i ++){
			int rank_tw = i;
			//get the rank_vw
			int rank_vw = -1;
			for(int j = 0; j < results_vw.length; j ++){
				if(results_vw[j].equals(results_tw[i])){
					rank_vw = j;
					break;
				}
			}
			if(rank_vw == -1){
			//	rank_vw = ext_rank_vw;
			//	ext_rank_vw ++;
				
				rank_vw = max_rank;
			}
			
			if(combined.containsKey(results_tw[i]) == false){
				double rank = lambda * rank_tw + (1 - lambda) * rank_vw;
				combined.put(results_tw[i], rank);
			}
		}
		
		//for each item in results_vw, get the score(if not included in combined yet)
		for(int i = 0; i < results_vw.length; i ++){
			if(combined.containsKey(results_vw[i]) == false){
				int rank_vw = i;
				//get rank_tw
				int rank_tw = -1;
				for(int j = 0; j < results_tw.length; j ++){
					if(results_tw[j].equals(results_vw[i])){
						rank_tw = j;
						break;
					}
				}
				if(rank_tw == -1){
				//	rank_tw = ext_rank_tw;
				//	ext_rank_tw ++;
					rank_tw = max_rank;
				}
				
				double rank = lambda * rank_tw + (1 - lambda) * rank_vw;
				combined.put(results_vw[i], rank);
			}
		}
		//sort the combined by value
		List<Entry<String, Double>> sorted = entriesSortedByValues(combined);
		String sorted_results[] = new String[sorted.size()];
		int i = 0;
		for(Entry<String, Double> e : sorted){
			sorted_results[i] = e.getKey();
			i ++;
		}
 		
		return sorted_results;
	}
	
	//hashmap sorting by value
	static <K,V extends Comparable<? super V>> 
    List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());

		Collections.sort(sortedEntries, 
				new Comparator<Entry<K,V>>() {
					@Override
					public int compare(Entry<K,V> e1, Entry<K,V> e2) {
		            return e1.getValue().compareTo(e2.getValue());
					}
		    	}
		);

		return sortedEntries;
	}
}
