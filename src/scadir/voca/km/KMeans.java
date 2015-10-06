package scadir.voca.km;

import java.io.IOException;
import java.util.Date;

import scadir.voca.hkm.HKM;

//interface should be same to TopDownclustering
public class KMeans {

	public static String runKmeansClustering(String features, String cluster_output, int k) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		
		long ts0 = new Date().getTime();
		HKM.topLevelProcess(features, cluster_output+"top/cls", cluster_output+"/top/", k);
		// merge the clusters into a single file
		try {
			
			HKM.merge(cluster_output, cluster_output);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long ts3 = new Date().getTime();
		HKM.log("kmeans clustering ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min");
		return "kmeans clustering ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min\n";
	}
}