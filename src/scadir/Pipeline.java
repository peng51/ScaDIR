package scadir;

import java.io.IOException;

import scadir.feat.FeatureExtraction;
import scadir.repr.Histogram;
import scadir.solr.Search;
import scadir.voca.VCDriver;

public class Pipeline {
	public static void main(String[] args) 
			throws IOException, InterruptedException, Exception{
		
		//test run
		String input_seqfile = args[0];
		String output_root = args[1];
		int clustering_type = Integer.parseInt(args[2]);
		int botleveltype = Integer.parseInt(args[3]);
		int TopK = Integer.parseInt(args[4]);
		int BotK = Integer.parseInt(args[5]);
		runPipeline(input_seqfile, output_root, clustering_type, botleveltype, TopK, BotK);

		//runPipeline("data/images_100.seq", "output_root", 0, 0, 10, 10);
	}

	
	/*
	 * @PARAM in_path_imageseq: input folder/file of seq files of images
	 * @PARAM output_root: output dir for feature extraction and clustering and frequecy(representation) output
	 * @PARAM clustering_type: 0 - HKM;   1 - Mahout kmeans;   2 - AKM.
	 * @PARAM botleveltype: valid only for HKM, bot level clustering parallelism: 
	 *                      0: serial; 1: map-reduce based parallelism, 2:  multi-thread parallelism
	 * @PARAM TopK
	 * @PARAM BotK
	 */
	public static void runPipeline(String in_path_imageseq, String output_root, int clustering_type, int botleveltype, int TopK, int BotK) 
			throws Exception, IOException, InterruptedException{
		String features = output_root + "/data/features";
		int cluster_num = TopK * BotK;
		
		//feature extraction (feature count will be extracted to data/feature_count folder)
		FeatureExtraction.extractFeatures(in_path_imageseq, features);
		
		//clustering
		String clustering_root = output_root + "/cluster";
		String[] args = {features, output_root, "" + TopK, "" + BotK};
		VCDriver.run(args, clustering_type, botleveltype);
		
		//representation
		Histogram.runJob(features, cluster_num, clustering_root + "/clusters.txt", output_root + "/tmp", output_root + "/data/frequency.txt");
		
		//indexing
		Search.init(output_root + "/data/frequency.txt", cluster_num, clustering_root + "/clusters.txt", TopK, BotK);
		Search.runIndexing(output_root + "/data/frequency.txt");
		
		//search
		String[] results = Search.search("data/all_souls_000000.jpg");
		
		//output results to stdout
		for(String str : results){
			System.out.println(str);
		}
		
	}
}
