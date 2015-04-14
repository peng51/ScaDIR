package scadir.feat;

/*
 * MapReduce implementation of SIFT Feature Extraction
 * Input: A dir of sequence files of images
 * Output: A folder of extracted features.
 */

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class FeatureExtraction {
	
	public static final int feature_length=128;
	public static final Integer split_size = (int) (1024*1024*128);//128MB
	
	
	/*
	 * entry for calling feature extraction
	 * @param in_path path to the Sequencefiles of images
	 * @param out_path outpath where the extracted features will be output.
	 */
	public static void extractFeatures(String in_path,  String out_path) 
			throws ClassNotFoundException, IOException, InterruptedException{
		
		extractMR(in_path, out_path);
		System.out.println("Feature extraction is done, featres output to " + out_path);
		
	}
	
	/*
	 * MR job for feature extraction
	 */
	public static void extractMR(String infile, String outfile) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		//pass the parameters
		conf.set("seqfile", infile);
		//conf.set("fn", fn);
		conf.set("feature_folder", outfile);
		conf.set("outfile","test");
		conf.set("mapred.max.split.size", split_size.toString());
		Job job = new Job(conf);
		job.setJobName("FeatureExtractionSeqFile");
		job.setJarByClass(FeatureExtraction.class);
		job.setMapperClass(FeatureExtraction.FEMap.class);
		
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
	
		System.out.println("\nFeatureExtraction: Setting number of reducer adaptively");
	    int default_num_reducer = 100;
		FileSystem fs = FileSystem.get(conf);
		ContentSummary cs = fs.getContentSummary(new Path(infile));
		long input_size = cs.getLength();//cs.getSpaceConsumed();
		default_num_reducer = (int)(Math.ceil( ((double)input_size)/(1024*1024*64) ));//50MB PER REducer
		System.out.println("Path: "+infile+" size "+input_size+", will use "+default_num_reducer+" reducer(s)\n\n");
		job.setNumReduceTasks(default_num_reducer);
		
		FileInputFormat.setInputPaths(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outfile));
		
		job.waitForCompletion(true);
	}
	
	/*
	 * Mapper for feature Extraction
	 */
	public static class FEMap extends  Mapper<Text,BytesWritable, Text, VectorWritable> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		
		//used for feature count
		static Path feature_count_path = null;
		static long feature_count = 0;
		static String featurecount_filename = null;
		
		static VectorWritable vw = new VectorWritable();
		static Vector vec = new DenseVector(feature_length);
		
		@Override
		public void setup( Context context) {
			Configuration conf = context.getConfiguration();
		   img_folder = conf.get("seqfile");
		   feature_folder = conf.get("feature_folder");
		   
		   //used for feature count
		   Path p = new Path(feature_folder);
		   Path parent = p.getParent();
		   feature_count_path = new Path(parent, "feature_count");	
		   feature_count = 0;
		}

		@Override
		public void map( Text key,BytesWritable value, Context context) throws IOException {
			
			String file = img_folder + "/" + key.toString();
			
			//assuming each pic has a different name
			if(featurecount_filename == null){
				featurecount_filename = new Path( key.toString()).getName();
			}
			try{
				byte[] image_bytes = value.getBytes();
				if(image_bytes.length>0){
					BufferedImage img = ImageIO.read(new ByteArrayInputStream(value.getBytes()));
					String[] features = SIFT.getFeatures(img);
					for(int i = 0; i < features.length; i++){
						double[]  feature = getPoints(features[i].split(" "), feature_length);
						
						vec.assign(feature);
						vw.set(vec);
						context.write(new Text(file), vw);
						feature_count ++;
					}
				}
			} catch (java.lang.IllegalArgumentException e){
				System.out.println("the image causing exception: " + file);
			}
			catch( java.awt.color.CMMException e){
				//
				System.out.println("the image causing exception: " + file);
			}
			catch(javax.imageio.IIOException e){
				System.out.println("the image causing exception: " + file);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(java.lang.ArrayIndexOutOfBoundsException e){
				//what can we do about the corrupted images?????
			}
			
		}
		
		/*
		 * write the feature_count to the feature_count_path/file_name(non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		protected void cleanup(Context context) throws IOException {
			if(feature_count == 0){
				return;
			}
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream writer = fs.create(new Path(feature_count_path, featurecount_filename));
			StringBuilder sb = new StringBuilder();
			sb.append("" + feature_count);
			byte[] byt = sb.toString().getBytes();
			writer.write(byt);
			writer.close();
		}

		/*
		 * Convert a String array of features to double array
		 */
		public static double[] getPoints(String[] args, int size){
			//System.out.println(args.length);
			double[] points = new double[size];
			for (int i = 0; i < size; i++)
				points[i] = Double.parseDouble(args[i+4]);
			return points;
		}
	}
}
