package scadir.solr;


import scadir.repr.Histogram;//ir.cluster.Frequency;
import scadir.voca.hkm.HKM;//ir.cluster.TopDownFrequency;
import scadir.feat.SIFT;//ir.feature.SIFTExtraction;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * Indexing and Searching runs locally using Solr
 */

public class Search {
	
	public static int featureSize = 128;
	
	public static int clusterNum = 100;
	public static int topclusterNum = 10;
	public static int botclusterNum = 10;
	public static String clusters = "test/cluster/clusters/";
	public static String terms = "data/features/frequency.txt";
	
	public static String urlString = "http://localhost:8989/solr";
	public static int numOfResults = 20;
	
	//cache 
	static double[][] topcluster = null;
	
	static HashMap<Integer, double[][]> botclusters = new HashMap<Integer, double[][]>();
	static final int max_number_botclusters = 200; //200 cache to store the bot level clusters
	//static int index_to_be_deleted = 0; // the  clusters to be deleted when hashmap is full
	
	public static void main(String[] args) throws IOException, SolrServerException{
		// run indexing
		//runIndexing("test/data/frequency.txt");
		//search("data/images/all_souls_000000.jpg");
//		loadConfiguration("test/conf.xml");
		//loadConfiguration_topdown("test/conf_new.xml");
		//System.out.println(terms);
		//System.out.println(clusters);
		//System.out.println(clusterNum);
		//Search.loadConfiguration_topdown("test/conf_new.xml");
		//Search.search_topdown("data/images/all_souls_000000.jpg");
		//test("/home/yp/Desktop/results/test14");
	}
	
	
	public static void init(String terms, int clusterNum, String clusters, int topclusterNum, int botclusterNum){
		Search.terms = terms;
		Search.clusterNum = clusterNum;
		Search.clusters = clusters;
		Search.topclusterNum = topclusterNum;
		Search.botclusterNum = botclusterNum;
		
		//init the static cache files -- important
		Search.topcluster =null;
		Search.botclusters.clear();
	}
	
	public static void loadConfiguration(String path){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			SAXReader reader = new SAXReader();
		    Document document = reader.read(fs.open(new Path(path)));
		    Element root = document.getRootElement();
		    Search.clusters = root.valueOf("@clusters");
		    Search.terms = root.valueOf("@terms");
		    Search.clusterNum = Integer.parseInt(root.valueOf("@clusterNum"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void loadConfiguration_topdown(String path){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			SAXReader reader = new SAXReader();
		    Document document = reader.read(fs.open(new Path(path)));
		    Element root = document.getRootElement();
		    Search.clusters = root.valueOf("@clusters");
		    Search.terms = root.valueOf("@terms");
		    Search.topclusterNum = Integer.parseInt(root.valueOf("@topclusterNum"));
		    Search.botclusterNum = Integer.parseInt(root.valueOf("@botclusterNum"));
		    Search.clusterNum = Search.topclusterNum * Search.botclusterNum;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//TODO: code cleaning and add an entry point function
	public static long runIndexing(String terms){
		long docs_indexed=-1;
		try {
			docs_indexed = Indexing.index(terms);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return docs_indexed;
	}
	
 	public static String[] search(String image){
		String[] results=null;
		try {
			System.out.println("test image: " + image);
			String[] features = getImageFeatures(image);
			String qs = Search.createQuery(features);
			results = query(qs);
			System.out.println("results length = " + results.length);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
		
	}
	
	// search using the topdown frequency approach
	public static String[] search_topdown(String image){
		String[] results=null;
		try {
			System.out.println("test image: " + image);
			String[] features = getImageFeatures(image);
			String qs = Search.createQuery_topdown(features);
			results = query(qs);
			System.out.println("results length = " + results.length);
			System.out.println(results[0] + "\n" + results[1] + "\n" + results[2] + "\n" + results[3] + "\n" + results[4] + "\n" + results[5]);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
		
	}
	
	
	public static String[] getImageFeatures(String image){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedImage img = ImageIO.read(fs.open(new Path(image)));
			String[] features = SIFT.getFeatures(img);
			return features;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static String createQuery(String[] features) throws IOException{//transform an image into a Solr document or a field

		double[][] clusters = Histogram.FreMap.readClusters(Search.clusters, Search.clusterNum);
		int[] marks = new int[Search.clusterNum];
			
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 4]);
			int index = Histogram.FreMap.findBestCluster(feature, clusters);
			marks[index]++;
		}
			
		String result = "";
		for(int i = 0; i < Search.clusterNum; i++){
			for(int j = 0; j < marks[i]; j++){
				if(result.length() == 0) result += i;
				else result += " " + i;
			}	
		}
		//System.out.println("query string: " + result);
		return result;
	}
	
	//for dealing with topdown frequency approach
	public static String createQuery_topdown(String[] features) throws IOException{//transform an image into a Solr document or a field
		String result = "";
		Search.topcluster = Histogram.FreMap.readClusters(Search.clusters + "/0/0.txt", topclusterNum);
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 4]);
			int index = findGlobalClusterId(feature, Search.clusters, Search.topclusterNum, Search.botclusterNum);
			
			if(result.length() == 0){
				result = result + index;
			}
			else{
				result = result + " " + index;
			}
		}
		
		return result;
	}

	public static int findGlobalClusterId(double[] feature, String clusters_folder, int topclusterNum, int botclusterNum) throws IOException{
		int topId = Histogram.FreMap.findBestCluster(feature, Search.topcluster);
		double[][] cs_bot = null;
		if(Search.botclusters.containsKey(topId)){//hit
			cs_bot = Search.botclusters.get(topId);
		}
		else {//miss
			cs_bot = Histogram.FreMap.readClusters(clusters_folder+"/1/" + topId + ".txt", botclusterNum);
			if(Search.botclusters.size() >= Search.max_number_botclusters){
				// Hashmap full, delete a random pair
				Random        random    = new Random();
				List<Integer> keys      = new ArrayList<Integer>(Search.botclusters.keySet());
				Integer       randomKey = keys.get( random.nextInt(keys.size()) );
				Search.botclusters.remove(randomKey);
			}
			Search.botclusters.put(topId, cs_bot);
		}
		int botId = Histogram.FreMap.findBestCluster(feature, cs_bot);
		return topId * botclusterNum + botId;
	}
	

	//query with customed number of results
	public static String[] query(String s, int num_results) throws SolrServerException{//query and output results
		
		//query a numeric vector as a string
		String urlString = Search.urlString;
		HttpSolrServer server = new HttpSolrServer(urlString);
		// search
	    SolrQuery query = new SolrQuery();
	    //query.setQuery("includes:" + s);
	    query.set("q", "includes:" + s);
	    query.setRows(num_results);
	    // get results		
	    QueryResponse qresponse = server.query(query);
	    SolrDocumentList list = qresponse.getResults();
	    String[] files = new String[list.size()];
	    for(int i = 0; i < list.size(); i++){
	    	//System.out.println(list.get(i).getFieldValue("id"));
	    	files[i] = list.get(i).getFieldValue("id").toString();
	    }
	    
	    return files;
	}
	
	//call with a default number of results
	public static String[] query(String s) throws SolrServerException{//query and output results
		
	    return query(s,Search.numOfResults);
	}
}