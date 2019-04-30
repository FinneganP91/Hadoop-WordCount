import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;


import java.net.URI;


import org.apache.hadoop.fs.FileSystem;



public class Tokenizer{
	


	public static class TokenMapper
    extends Mapper<Object, Text, Text, IntWritable>{

 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
     word.set(itr.nextToken());
     context.write(word, one);
   }
 }
}
	
	public static class SumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	     sum += val.get();
	   }
	   result.set(sum);
	   context.write(key, result);
	 }
	}
	
	public static void TweetJob() throws Exception {
		
		
	    System.setProperty("hadoop.home.dir", "/");   
	    System.setProperty("HADOOP_USER_NAME", "hdfs");
		Configuration conf = new Configuration();
		
		
		conf.set("mapred.textoutputformat.separator", ",");	
		Job job = Job.getInstance(conf, "Tokenizer");
		
		job.setJarByClass(Tokenizer.class);
		job.setMapperClass(TokenMapper.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://0.0.0.0:19000/filestore/tweets/"));
		FileOutputFormat.setOutputPath(job, new Path("C:/HadoopWordCount/output/"+ WordCount.JobCount +"/"));
	
		job.waitForCompletion(true);
						
		WriteToHdfs(conf);
	
		System.out.println("Files written into datanode job: "+WordCount.JobCount+ "\r");
		System.out.println("**********************************WordCount output**********************************\r");
		ReadFile();
		
		System.exit(0);
		
		
		
	
	}
	
	public static void ReadFile() throws Exception {
		
		Configuration conf = new Configuration();
		
		Path pt=new Path("hdfs://0.0.0.0:19000/filestore/tweetoutput.txt");
		FileSystem fs = FileSystem.get(new URI("hdfs://0.0.0.0:19000"), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String line;
		
		try {
		 
		  while ((line = br.readLine()) != null) {
			  
			
			  String[] parts = line.split(",", -1);
			  		
		        if (parts.length >= 2)
		        {
		            String key = parts[0];
		            Integer value = Integer.valueOf(parts[1]);
		            map.put(key, value);
		            
		        }
		  }
		  
		 
		  List<Map.Entry<String, Integer> > list = 
	               new LinkedList<Map.Entry<String, Integer> >(map.entrySet()); 
		  
		  Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() { 
	            public int compare(Map.Entry<String, Integer> o1,  
	                               Map.Entry<String, Integer> o2) 
	            { 
	                return (o1.getValue()).compareTo(o2.getValue()); 
	            } 
	        });
		  
		  for(Entry<String, Integer> x  : list) {
			 
			  System.out.println(x.getKey() + " " +  x.getValue());
			  
		  }
		  System.out.println("\r\r");
		  
		} finally {
		
		  br.close();
		  System.out.println("************************************************************************************ \r");
		  System.out.println("End of file");
		}
	}
			
	public static void WriteToHdfs(Configuration conf)throws Exception {
		
		  System.setProperty("hadoop.home.dir", "/");   
	      System.setProperty("HADOOP_USER_NAME", "hdfs");  
	     
	      InputStream inputStream = new BufferedInputStream(new FileInputStream("c:/HadoopWordCount/output/"+WordCount.JobCount + "/part-r-00000"));  
	  
	      FileSystem hdfs = FileSystem.get(new URI("hdfs://0.0.0.0:19000"), conf);  
	     
	      OutputStream outputStream = hdfs.create(new Path("hdfs://0.0.0.0:19000/filestore/tweetoutput.txt"),   
	      new Progressable() {  
	              @Override
	              public void progress() {
	                System.out.println("....");
	              }
	        });
	      try
	      {
	        IOUtils.copyBytes(inputStream, outputStream, 4096, false); 
	      }
	      finally
	      {
	        IOUtils.closeStream(inputStream);
	        IOUtils.closeStream(outputStream);
	      } 
	}
	
	public static void WriteTweet(String tweet) throws Exception{
	
			
	      File.createTempFile("tweet", "txt");
	      BufferedWriter write = new BufferedWriter(new FileWriter("tweet"+ WordCount.TweetCount + ".txt", true));
	      write.write(tweet);
	      write.close();
	      
	      Configuration conf = new Configuration();
		 
			
		 System.setProperty("hadoop.home.dir", "/");   
	     System.setProperty("HADOOP_USER_NAME", "hdfs");  
	     
	     InputStream inputStream = new BufferedInputStream(new FileInputStream("tweet"+ WordCount.TweetCount + ".txt"));  
	 
	     FileSystem hdfs = FileSystem.get(new URI("hdfs://0.0.0.0:19000"), conf);  
	    
	     OutputStream outputStream = hdfs.create(new Path("hdfs://0.0.0.0:19000/filestore/tweets/tweet"+WordCount.TweetCount+".txt"));
	    
	     try
	     {
	       IOUtils.copyBytes(inputStream, outputStream, 4096, false); 
	     }
	     finally
	     {
	       IOUtils.closeStream(inputStream);
	       IOUtils.closeStream(outputStream);
	     } 
	}
	

		
}

	
	


	



