package scadir.feat;
/*
 * SIFT Feature Extraction using LIRE package
 * LIRE: http://www.semanticmetadata.net/lire/
 */
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import net.semanticmetadata.lire.imageanalysis.sift.Extractor;
import net.semanticmetadata.lire.imageanalysis.sift.Feature;

public class SIFT {
	private static Extractor extractor = new Extractor();
	
	/**
	 * main function for test
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedImage img = ImageIO.read(fs.open(new Path("data/images/all_souls_000073.jpg")));
		getFeatures(img);
	}	
		
	/**
	 * Return SIFT feature in String Array.
	 * @param img the BufferedImage of the picture
	 */
	public static String[] getFeatures(BufferedImage img) throws IOException {
		List<Feature> features = extractor.computeSiftFeatures(img);
		String[] results = new String[features.size()];
		for(int i = 0; i < features.size(); i++){
			String s = features.get(i).scale + " " + features.get(i).orientation + " " + features.get(i).location[0] + 
						" " + features.get(i).location[1] + " " + features.get(i).toString(); 
			results[i] = s;
		}
		//System.out.println("the number of features = " + features.size());
		return results;
	}
}
