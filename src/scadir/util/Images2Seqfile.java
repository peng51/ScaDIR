package scadir.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/*
 * convert a folder of images to a seqfile on local file system.
 * input: folder
 * output: a seqfile
 * 
 */
public class Images2Seqfile {
	private static BytesWritable value=new BytesWritable();
	private static Text key = new Text();
	private static int max_length=1024*1024*40; //max 40MB image file
	private static byte[] images_raw_bytes=new byte[max_length];

	    public static void main( String[] args)  {
	    	
	    	
	    	String input = args[0];
	        String output = args[1];
	    	
	    	long startTime = new Date().getTime();	        
	        Configuration conf = new Configuration();
	        
	        SequenceFile.Writer writer = null;
	        try {
	        	FileSystem fs = FileSystem.get(URI.create( output), conf);
		        Path outfile=new Path(output);
		        
		      //  String[] allfiles=HadoopUtil.getListOfFiles(args[0]);
		        //get all picture files in the dir to the collections
		        Collection<File> files = FileUtils.listFiles(
		        		  new File(input), 
		        		  new RegexFileFilter("([^\\s]+(\\.(?i)(jpg|JPG|JPEG|png|PNG|gif|GIF|bmp|BMP))$)"), 
		        		  DirectoryFileFilter.DIRECTORY
		        		);
		       ArrayList<String> allfiles = new ArrayList<String>();
		       for(File f : files){
		    	  allfiles.add( f.getAbsolutePath() );
		    	  
		    	  //debug
		    	  //System.out.println(f.getAbsolutePath());
		       }
		       System.out.println("# images: " + allfiles.size());
	        	writer = SequenceFile.createWriter( fs, conf, outfile, key.getClass(), value.getClass());

	            int files_processed=0;
	            for(String file:allfiles){
	            	if(file.endsWith("~")==false){
		            	FSDataInputStream fileIn = fs.open(new Path(file));
		    			int bytes_read=fileIn.read(images_raw_bytes);
		    			
		    			File f = new File(file);
		    			String parentfolder = getParentName(f);
		    			key.set(parentfolder + "/" + f.getName());
//		    			System.out.println(f.getAbsolutePath() + "\t" + key);
		    		//	System.out.println("file:"+file+"\nbytes_read"+bytes_read);
		    		//	System.out.println(bytes_read);
		    			if(bytes_read == -1)
		    				continue;
		    			value.set(images_raw_bytes, 0, bytes_read);
		    			writer.append(key, value);
		    			fileIn.close();
		    			files_processed++;
		    			if(files_processed%3000==0){
		    				System.out.println(files_processed+"/"+allfiles.size()+ "  have been processed");
		    				
		    				//System.out.println("Sleepting for 3 secs");
							//Thread.sleep((long) (1000*0.01));
		    			}
	            	}
	            }
	            
	        }
	        catch(IOException e){
	        	e.printStackTrace();
	        }
	        finally 
	        { IOUtils.closeStream( writer); 
	        }
	        System.out.println("all files processed.");
	    	long EndTime = new Date().getTime();
			double time_secs=(double)(startTime-EndTime)/1000;
			System.out.println("time spent: "+time_secs+" seconds");
			FileWriter fw;
			try {
				fw = new FileWriter("Image2Seqfile_runningtime.txt",true);
				fw.write("input:" + args[0]+"  time spent : "+time_secs+" seconds\n\n");//appends the string to the file
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //the true will append the new data
			
	    } 
	    
	    public static String getParentName(File file) {
	        if(file == null || file.isDirectory()) {
	                return null;
	        }
	        String parent = file.getParent();
	        parent = parent.substring(parent.lastIndexOf("/") + 1, parent.length());
	        return parent;      
	    }
}
