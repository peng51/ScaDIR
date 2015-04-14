package scadir.util;
/*
 * File Util functions for ScaDIR
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
	public static void main(String[] args) throws IOException{
		String input = args[0];
		String suffix = args[1];
		String[] files = HadoopUtil.getListOfFiles(input);
		FileSystem fs = FileSystem.get(new Configuration());
		
		for(String file : files){
			fs.rename(new Path(file), new Path(file + suffix));
		}
	}
	
	/*
	 * copy text files in an folder to a big text file
	 */
	public static void copyMerge(String folder, String file){
		Path src = new Path(folder);
		Path dst = new Path(file);
		Configuration conf = new Configuration();
		try {
			FileUtil.copyMerge(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * delete a path
	 */
	public static void delete(String dirPath){
		Path path = new Path(dirPath);
		Configuration conf = new Configuration();
		try {
			FileUtil.fullyDelete(path.getFileSystem(conf), path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * create a dir
	 */
	public static void mkdir(String dirPath){
		Path path=new Path(dirPath);
		Configuration conf = new Configuration();
		try {
			path.getFileSystem(conf).mkdirs(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to create directory : "+dirPath);
			e.printStackTrace();
		}
	}
	
	/*
	 * Copy a dir recursively
	 */
	public static void cpdir(String srcPath, String dstPath){
		//minor fix on this line
		Path src=new Path(srcPath+"/");
		Path dst=new Path(dstPath);
		Configuration conf = new Configuration();
		try {
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst,false, true, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to cp directory : "+srcPath + " to "+dstPath);
			e.printStackTrace();
		}
	}
	
	/*
	 * Copy a File
	 */
	public static void cpFile(String srcFile, String dstFile){
		Path src = new Path(srcFile);
		Path dst = new Path(dstFile);
		Configuration conf = new Configuration();
		try {
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, true, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to cp File : " + srcFile + " to " + dstFile);
			e.printStackTrace();
		}
	}
	
	/*
	 * get list of files, filtering out filenames starting with "_"
	 */
	public static String[] getListOfFiles(String folder_path){
		ArrayList<String> ListOfFolders=new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(folder_path));
			for (FileStatus filestatus : status){
				if(filestatus.isDir() == false && filestatus.getPath().getName().startsWith("_") == false)
					ListOfFolders.add(folder_path+"/"+filestatus.getPath().getName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] res = new String[ListOfFolders.size()];
		res = ListOfFolders.toArray(res);
		return res;
	}
	
	/*
	 * Return a String array of the folders in the input directory
	 */
	public static String[] getListOfFolders(String folder_path){
		ArrayList<String> ListOfFolders=new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(folder_path));
			for (FileStatus filestatus : status){
				if(filestatus.isDir() == true && filestatus.getPath().getName().startsWith("_") == false)
					ListOfFolders.add(folder_path + "/" + filestatus.getPath().getName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] res = new String[ListOfFolders.size()];
		res = ListOfFolders.toArray(res);
		return res;
	}
}
