package scadir.multimodal;

import scadir.util.HadoopUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


//input[1]: multimodal images root folder (with the map.txt in its subfolders)(dont need the images in them though)
//input[2]: clustering frequency results of the output folder
// vw.txt, tw.txt, vw_tw.txt(VW only, TW only, VW&TW histogram will be stored in VW_output_root/data folder)
public class MultiFreq {

	
	public static void run(String images_root, String VW_output_root, HashMap<String, Integer> num_per_category)
			throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] folders = HadoopUtil.getListOfFolders(images_root);
		
		//get all the text words for each image, store them in hashmap: key = foldername/imagename(without suffix"jpg or png."), value = words
		HashMap<String, String> all_tw = new HashMap<String, String>();
		for(String folder : folders){
			Path folder_path = new Path(folder + "/map.txt");
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(folder_path)));
			int lineno = 0;
			String inline = null;
			while((inline = br.readLine()) != null){
				String[] splits = inline.split("---");
				
				//for mmd-2
				//String value = splits[0];
				//String key = folder_path.getParent().getName() + "/" + lineno;
				
				//for mmd-3
				String value = splits[1].trim();
				//--------------------------fix empty string error
				int i = 1;
				while(value.trim().length() == 0){
					i ++;
					value = splits[i];
				}
				//------------------------------------------------
				String key = folder_path.getParent().getName() + "/" + splits[0].trim().split("\\.")[0];
				
				lineno ++;
				all_tw .put(key, value);
			}
			br.close();
		}
		
		//traverse the frequency.txt and output to tw.txt, vw.txt, vw_tw.txt
		String vw_file = VW_output_root + "/data/vw.txt";
		String tw_file = VW_output_root + "/data/tw.txt";
		String vw_tw_file = VW_output_root + "/data/vw_tw.txt";
		
		BufferedWriter bw_vw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(vw_file),true)));
		BufferedWriter bw_tw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(tw_file),true)));
		BufferedWriter bw_vw_tw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(vw_tw_file),true)));
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(VW_output_root + "/data/frequency.txt"))));
		String inline = null;
		while((inline = br.readLine()) != null){
			String[] splits = inline.split("\\s+");
			String vw = "";
			int num_vw = splits.length - 2;
			for (int i = 2; i < splits.length; i ++){
				vw += " vw" + splits[i];
			}
			String[] filename_splits = splits[0].split("/");
			String category = filename_splits[filename_splits.length -2];
			String key = category + "/" + filename_splits[filename_splits.length - 1].split("\\.")[0];
			String tw = all_tw.get(key);
			
			//debug 
			//System.out.println(tw);
			if(tw == null){
				System.out.println("!!!");
				continue;
			}
			
			int num_tw = tw.split("\\s+").length;
			bw_vw.write(splits[0] + "\t" + num_vw + "\t" + vw + "\n");
			bw_tw.write(splits[0] + "\t" + num_tw + "\t" + tw + "\n");
			bw_vw_tw.write(splits[0] + "\t" + (num_vw + num_tw) + "\t" + vw + " " + tw + "\n");
			
			if(num_per_category.containsKey(category) == false){
				num_per_category.put(category, 1);
			}
			else{
				int num = num_per_category.get(category);
				num_per_category.remove(category);
				num_per_category.put(category, num + 1);
			}
		}
		bw_vw.flush(); bw_vw.close();
		bw_tw.flush(); bw_tw.close();
		bw_vw_tw.flush(); bw_vw_tw.close();
		br.close();
	}
}