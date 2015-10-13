package scadir.multimodal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import scadir.feat.FeatureExtraction;
import scadir.repr.Histogram;
import scadir.solr.Search;
import scadir.util.HadoopUtil;
import scadir.voca.VCDriver;


public class Twitter_pipeline {
	
	public static void main(String args[]) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException, SolrServerException{

		/**FE and Clustering**/
		/***********************/
/*		String input = args[0]; // input of a folder of seqfiles of features
		String output = args[1]; //output of extracted features and clustering result
		
		
		int topk = 100; //Integer.parseInt(args[2]);
		int botk = 100; //Integer.parseInt(args[3]);
		
		String[] input_seqfiles = HadoopUtil.getListOfFiles(input);
		
		int cluster_type = 0;
		int botlvl_type = 2;
		
		for(String input_seqfile : input_seqfiles){
			
			run_clustering(input_seqfile, output + "/" + new File(input_seqfile).getName(), topk, botk, cluster_type, botlvl_type);
		}
*/		
	
		
		/**indexing and search**/
		/**********need to do step after step!!!!***********/
		
		//step 1: get VW only and TW only search results in folder "Twitter_query_output_hkm10k"
		String search_time_result = search("Twitter_query_resized" , "Twitter_query_output_hkm10k", "TwitterImages", "Twitter_output/akm_10000", 100, 100);
		
		//Step 2: annotate the top 5 results, check if they are relevant or not. 1 means yes, 0 means no
		
		//step 3: get the combined search results 
		String combined_search_time = combineResults("Twitter_query_output_hkm10k", "Twitter_combined_output", "TwitterImages");
		System.out.println(search_time_result + "\n" + combined_search_time);
		
		//step 4 annotate the combined top 5 results like in step 2
		
		//step 5: get the summary of top 5 accuracy for VW, TW, and combined search
		getAnnotatedResultSummary("Twitter_query_output");
		
	}
	
	//clustering: given a input seqfile of images, run feature extraction and clustering 
	
	/*@PARAM
	 * src :  input of extracted features 
	 * dst: output (clustering results)
	 * topk:
	 * botk:
	 * cluster_type: 0 topdown clustering,  1 normal kmeans, 2  AKM
	 * botlvl_type: 0 MR based, 1 multithread based; valid only for topdown clustering 
	 */
	public static String run_clustering(String src, String dst, int topK, int botK, int cluster_type, int botlvl_type ) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		HadoopUtil.delete(dst);
		
		Path srcPath = new Path(src);
		
		//Feature Extraction
		System.out.println("\n\nFeature Extraction");
		String features = dst + "/data/features";// the feature folder
		//FeatureExtraction.extractFeatures(src, features, dst + "/temp/fe/");
		FeatureExtraction.extractFeatures(src, features);
		System.out.println("Features folder:" + features);

		long EndTime1 = new Date().getTime();

		
		//Vocabulary Construction and Frequency Generation
		System.out.println("\n\nvocabulary construction and frequency generation");
		String[] args = {features, dst, "" + topK, "" + botK};
		String s = VCDriver.run(args,  cluster_type, botlvl_type);
		long EndTime2 = new Date().getTime();
		
		//Indexing and Searching
		System.out.println("\n\nIndexing and Searching");
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done 
		int clusterNum = topK * botK;
		
		//old frequency approach
		//Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt", topK, botK);
		//long total_images = Search.runIndexing(dst + "/data/frequency.txt");
				
		//Search.init(dst + "/data/frequency_new.txt", clusterNum, dst + "/cluster/clusters", topK, botK);
	    //long total_images = Search.runIndexing(dst + "/data/frequency_new.txt");
		
		
		
		long EndTime3 = new Date().getTime();
		 
		System.out.println("\n\n*******************************************  Running Time in minutes ********************************************");
		
		System.out.println("Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				);

		String string_result="Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				;
		return string_result;
	}
	
	/*
	 * input: a folder of search images and a file named "queries.txt" 
	 * output: 
	 *      "top5":  a folder of search top 5 returned for each image
	 *      	vw: a folder for each query images, contains a txt containing image ids and its text, and the images in the folder
	 *          tw: folder ...
	 *          
	 *      "rankedlist" : a folder of whole searched ranked list
	 *      	each query image
	 */
	public static String search(String query_folder, String output, String images_root, String clustering_output, int topk, int botk) 
			throws IOException, SolrServerException{
		//get the tw and vw 
		MultiFreq.run(images_root, clustering_output, new HashMap<String, Integer>());
		File output_file = new File(output);
		if(output_file.exists() == false){
			output_file.mkdir();
		}
		String vw_time_result = search_pre(query_folder, output, images_root, clustering_output, topk, botk, 0);
		String tw_time_result = search_pre(query_folder, output, images_root, clustering_output, topk, botk, 1);
		
		return "vw : " + vw_time_result + "\ntw: " + tw_time_result;

		
		
	}
	
	//pre manual annotation data generation
	// will generate top5 results for both vw only and tw only query results
	//need human annotating if those results are correct or not.
	//type 0: vw only, type 1: tw only
	//input : query folder; images root dir, clustering output dir, topk botk, type
	//output: 
	/*
	 * 	 * output: 
	 *      "top5":  a folder of search top 5 returned for each image
	 *      	vw: a folder for each query images, contains a txt containing image ids and its text, and the images in the folder
	 *          tw: folder ...
	 *          
	 *      "rankedlist" : a folder of whole searched ranked list
	 *      	each query image
	 */
	public static String search_pre(String query_folder, String output, String images_root, String clustering_output, int topk, int botk, int type) 
			throws IOException, SolrServerException{
		mkdir(output + "/top5");
		mkdir(output + "/rankedlist");
		
		int clusterNum = topk * botk;
		String indexfile = null;
		
		String output_top5 = null;
		String output_rankedlist = null;
		if(type == 0){
			indexfile = clustering_output + "/data/vw.txt";
			
			output_top5 = output + "/top5/vw/";
			mkdir(output_top5);
			output_rankedlist = output + "/rankedlist/vw/";
			mkdir(output_rankedlist);
		}
		else{
			indexfile = clustering_output + "/data/tw.txt";
			
			output_top5 = output + "/top5/tw/";
			mkdir(output_top5);
			output_rankedlist = output + "/rankedlist/tw/";
			mkdir(output_rankedlist);
		}
		
		 long startTime = System.currentTimeMillis();
		Search.init(indexfile, clusterNum, clustering_output + "/cluster/clusters.txt", topk, botk);
		int indexed_images = (int) Search.runIndexing(indexfile);
		 long endTime = System.currentTimeMillis();
		double index_time = (double)(endTime - startTime) / 1000;
		
		//process each image as query
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path folder_path = new Path(query_folder + "/queries_less.txt");
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(folder_path)));
		
		String inline = null;
		int num_query_img = 0;
		startTime = System.currentTimeMillis();
		while((inline = br.readLine()) != null){
			
			//category is for folder path
			//search tw is the keyword for search!!
			String[] splits = inline.split("---");
			String category = splits[1].trim();
			String search_tw = splits[0].trim();
			splits = splits[2].trim().split("\\s+");
			int start_index = 0;
			
			
			for(int i = start_index; i < splits.length; i ++){
				num_query_img ++;
				
				String query_img = query_folder + "/" + splits[i].trim();
				double ap = Integer.MIN_VALUE;
				
				String qs = "";
				if(type == 0){
					//create the query (with only vw)
					qs = createQuery(query_img);
				}
				else{
					qs = search_tw;
				}
				
				System.out.println("QueryImage: " + query_img + ", query string: " + qs);
					
				//get the results from solr
				String[] results = Search.query(qs, indexed_images);
				

				//write top5 results plus the images, and text to output/top5/vw|tw/category/queryimg.txt
				writeTop5(results, images_root, output_top5, category, splits[i].trim());
				
				//wirte the results[] to output/rankedlist/vw|tw/category
				mkdir(output_rankedlist + "/" + category);
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output_rankedlist + "/" + category + "/" + splits[i].trim() + ".txt")));
				for(String s : results){
					bw.write(s + "\n");
				}
				bw.flush();
				bw.close();
				
				
			}
		}
		br.close();
		endTime = System.currentTimeMillis();
		double search_time = (double)(endTime - startTime) / 1000;
		
		return "index time: " + index_time +"\t num query images: " + num_query_img + "\t search time total: " + search_time;
		
		
	}
	//<tweetid => text>
	static HashMap<Long, String> tweetid2txt = null;
	
	//write top 5 results to the output_top5/category folder plus the img and text
	private static void writeTop5(String[] results, String images_root, String output_top5, String category, String query_img) 
			throws IOException {
		//init tweetid2txt the first time.
		if(tweetid2txt == null){
			tweetid2txt = new HashMap<Long, String> ();
		    File directory = new File(images_root);
		    // get all the files from a directory
		    File[] fList = directory.listFiles();
		    for (File file : fList) {
		        if (file.isDirectory()) {
		            BufferedReader br = new BufferedReader(new FileReader(new File(images_root + "/" + file.getName() +"/map.txt")));
		            String inline = null;
		            while((inline = br.readLine()) != null){
		            	String splits[] = inline.split("---");
		            	splits[0] = splits[0].trim();
		            	String str_tweetid = splits[0].substring(0, splits[0].length() - 4);
		            	long tweetid = Long.parseLong(str_tweetid);
		            	String txt = splits[1].trim();
		            	tweetid2txt.put(tweetid, txt);
		            }
		            
		            br.close();
		        }
		    }
		}
		mkdir(output_top5 + "/" + category);
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output_top5 + "/" + category + "/" + query_img + ".txt")));
		for(int i = 0; i < 5; i ++){
			String result = results[i];
			String splits[] = result.split("/");
			String img = splits[splits.length - 1].trim(); // "608866786876903424.jpg"
			String parent_folder = splits[splits.length - 2]; // "Eiffel-Tower"
			
			long tweetid = Long.parseLong(img.substring(0, img.length() - 4));
			String text = tweetid2txt.get(tweetid);
			
			
			
			bw.write("\t" + i + "\t" + tweetid + "\t" + text + "\n");
			
			Files.copy(new File(images_root + "/" + parent_folder + "/" + img).toPath(), 
					new File(output_top5 + "/" + category + "/" + query_img + "__" + i + ".jpg").toPath());
			
		}

		bw.flush();
		bw.close();
	}

	/*
	 * combine the results from vw only and tw only results
	 * 
	 * input: output of search folder
	 * outpt: 1. accuracy summary(to std)
	 *        2. output folder of combinedResults, top5 results only (need human checking to get the accuracy)
	 * 
	 */
	public static String combineResults(String search_output_root, String output, String images_root) throws IOException{
		mkdir(output);
		//get top 5 accuracy for each image of vw and tw
		HashMap<String, Double> vw_acc = new HashMap<String,Double>();
		HashMap<String, Double> tw_acc = new HashMap<String,Double>();
		getAcc(vw_acc, search_output_root + "/top5/vw/");
		getAcc(tw_acc, search_output_root + "/top5/tw/");
		
		 long startTime = System.currentTimeMillis();
		
		//get ranked list for each query image
		HashMap<String, String[]> rankedlist_vw = new HashMap<String, String[]>();
		HashMap<String, String[]> rankedlist_tw = new HashMap<String, String[]>();
		getRankedList(rankedlist_vw, search_output_root + "/rankedlist/vw/");
		getRankedList(rankedlist_tw, search_output_root + "/rankedlist/tw/");
		
		//for each images, get the combined list and write top 5 to output
		
		for(String img : rankedlist_vw.keySet()){
			double img_acc_vw = vw_acc.get(img);
			double img_acc_tw = tw_acc.get(img);
//			double img_acc_vw = 1;
//			double img_acc_tw = 1;
			
			//System.out.println(img_acc_vw + "\t" + img_acc_tw);
			double multiplyfactor = 1.0;
			double lambda  =  (multiplyfactor) * (img_acc_tw / (img_acc_tw + img_acc_vw));
			
			if(Double.isNaN(lambda)){
				lambda = 0.5;
			}
			System.out.println(lambda);
			String img_rankedlist_vw[] = rankedlist_vw.get(img);
			String img_rankedlist_tw[] = rankedlist_tw.get(img);
			String[] combined_results = Pipeline_mmd.getCombinedResults(img_rankedlist_tw, img_rankedlist_vw, lambda);
			
			writeTop5(combined_results, images_root, output, img, img);
		}
		long endTime = System.currentTimeMillis();
		double search_time = (double)(endTime - startTime) / 1000;
		return "combined search time : " + search_time;
/*		
		HashMap<String, Double> combined_acc = new HashMap<String,Double>();
		getAcc(combined_acc, output);


		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + "/accuracy_sum.txt")));
		int count = 0;
		double vw_acc_sum = 0;
		double tw_acc_sum = 0;
		double combined_sum = 0;
		for (String key : vw_acc.keySet()) {
			bw.write(key + "\t vw: \t" + vw_acc.get(key) + "\n");
			bw.write(key + "\t tw: \t" + tw_acc.get(key) + "\n");
			bw.write(key + "\t combined: \t" + combined_acc.get(key) + "\n\n");
			vw_acc_sum += vw_acc.get(key);
			tw_acc_sum += tw_acc.get(key);
			combined_sum += combined_acc.get(key);
			count ++;
		}
		bw.write("\n\nSumarry:\nvw: \t" + vw_acc_sum / count + "\ntw: \t" + tw_acc_sum / count + "\ncombined: \t" + combined_sum / count);
		bw.flush();
		bw.close();
*/		
	}
	
	
	/*
	 * get the acc for top5: vw, tw and combined, output the result sumary to file accuracy_sum.txt
	 */
	public static void getAnnotatedResultSummary(String search_output_root) throws IOException{
		//get top 5 accuracy for each image of vw and tw
		HashMap<String, Double> vw_acc = new HashMap<String,Double>();
		HashMap<String, Double> tw_acc = new HashMap<String,Double>();
		getAcc(vw_acc, search_output_root + "/top5/vw/");
		getAcc(tw_acc, search_output_root + "/top5/tw/");
		

		
		//get ranked list for each query image
		HashMap<String, String[]> rankedlist_vw = new HashMap<String, String[]>();
		HashMap<String, String[]> rankedlist_tw = new HashMap<String, String[]>();
		getRankedList(rankedlist_vw, search_output_root + "/rankedlist/vw/");
		getRankedList(rankedlist_tw, search_output_root + "/rankedlist/tw/");
		
		String output = search_output_root + "/top5/combinedResult";
		HashMap<String, Double> combined_acc = new HashMap<String,Double>();
		getAcc(combined_acc, output);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + "/accuracy_sum.txt")));
		int count = 0;
		double vw_acc_sum = 0;
		double tw_acc_sum = 0;
		double combined_sum = 0;
		for (String key : vw_acc.keySet()) {
			bw.write(key + "\t vw: \t" + vw_acc.get(key) + "\n");
			bw.write(key + "\t tw: \t" + tw_acc.get(key) + "\n");
			bw.write(key + "\t combined: \t" + combined_acc.get(key) + "\n\n");
			vw_acc_sum += vw_acc.get(key);
			tw_acc_sum += tw_acc.get(key);
			combined_sum += combined_acc.get(key);
			count ++;
		}
		bw.write("\n\nSumarry:\nvw: \t" + vw_acc_sum / count + "\ntw: \t" + tw_acc_sum / count + "\ncombined: \t" + combined_sum / count);
		bw.flush();
		bw.close();
	}
	
	//input : input_folder query_out_root/rankedlist/vw|tw/
	//output: hashmap rankedlist
	public static void getRankedList(HashMap<String, String[]> rankedlist, String input_folder) throws IOException{
		String folders[] = HadoopUtil.getListOfFolders(input_folder);
		for(String folder : folders){
			String files[] = HadoopUtil.getListOfFiles(folder);
			String input_txt = null;
			for(String file : files){
				if(file.endsWith(".txt")){
					//get the query img as key
					input_txt = file;
					String splits[] = input_txt.split("/");
					input_txt = splits[splits.length - 1];
					String query_img = input_txt.substring(0, input_txt.length() - 4);
					
					//get the arrarylist of ranked list
					ArrayList<String> list = new ArrayList<String>();
					BufferedReader br = new BufferedReader(new FileReader(new File(file)));
					String inline = null;
					while((inline = br.readLine()) != null){
						list.add(inline);
					}
					br.close();
					
					//convert arraylist to array
					String[] array = new String[list.size()];
					array = list.toArray(array);
					
					//add the array to hashmap
					rankedlist.put(query_img, array);
				}
			}
			
		}
	}
	
	//input : input_folder query_out_root/top5/vw|tw/
	//output: hashmap acc
	public static void getAcc(HashMap<String, Double> acc, String input_folder) throws IOException{
		String folders[] = HadoopUtil.getListOfFolders(input_folder);
		for(String folder : folders){
			String files[] = HadoopUtil.getListOfFiles(folder);
			String input_txt = null;
			for(String file : files){
				if(file.endsWith(".txt")){
					input_txt = file;
					String splits[] = input_txt.split("/");
					input_txt = splits[splits.length - 1];
					String query_img = input_txt.substring(0, input_txt.length() - 4);
					BufferedReader br = new BufferedReader(new FileReader(new File(file)));
					String inline = null;
					double total = 0;
					double correct = 0;
					while((inline = br.readLine()) != null){
						total ++;
						splits = inline.split("\t");
						
						System.out.println(file);
						
						if(Integer.parseInt(splits[0]) == 1){
							correct ++;
						}
					}
					br.close();
					acc.put(query_img, correct/total);
				}
			}
			
		}
	}
	public static void mkdir(String path){
		File f = new File(path);
		if(f.exists() == false)
			f.mkdir();
	}
	
	
	//create query from a given image
	static double[][] clusters = null;
	public static String createQuery(String img_path) throws IOException{//transform an image into a Solr document or a field
		
		if(clusters == null){
			clusters = Histogram.FreMap.readClusters(Search.clusters, Search.clusterNum);
		}
		
		int[] marks = new int[Search.clusterNum];
		double [][] all_features = null;
		
		//get features into double array all_features
		if(false){
//			all_features = SIFT.extractSIFT(img_path);
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
	
}

