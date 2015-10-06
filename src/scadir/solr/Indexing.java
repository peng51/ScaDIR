package scadir.solr;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * Indexing runs locally using Solr
 */

public class Indexing {
	public static long doc_buffer_size=5*5000;
	
	public static void main(String[] args) throws IOException, SolrServerException{
		//index("data/index/visual-word-frequency.txt");		
	}
	
	public static long index(String filename) throws IOException, SolrServerException{//indexing existing index matrix
		
		String urlString = Search.urlString;
		HttpSolrServer server = new HttpSolrServer(urlString);
		server.deleteByQuery( "*:*" );//clean the data in server
		long docs_total_size=0;
		//read index matrix from file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path infile=new Path(filename);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		String line;
		while((line = br.readLine()) != null){
			SolrInputDocument doc = getDocument(line);
			docs.add(doc);
			if(docs.size() >= doc_buffer_size){
				server.add(docs);
			    server.commit();
			    docs_total_size = docs_total_size+docs.size();
			    System.out.println("indexed  " + (docs.size()) + " docs");
				docs.clear();
			}
		}
		br.close();
		server.add(docs);
	    server.commit();
	    docs_total_size=docs_total_size+docs.size();
	    System.out.println("indexing is done, total docs indexed: "+docs_total_size);
	    return docs_total_size;
	}
	
	//for each line, construct an document
	public static SolrInputDocument getDocument(String line){
		SolrInputDocument doc = new SolrInputDocument();
		// add the id field
		String name = line.split("\t")[0];
		doc.addField("id", name);
		// add the cluster fields
		// index a numeric vector as a string
		
//		System.out.println(line);
		String s = line.split("\t")[2];
		// includes field = term vector
		doc.addField("includes", s);
		return doc;
	}
	
}
