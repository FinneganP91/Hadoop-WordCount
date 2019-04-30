

import java.io.File;

import java.io.FileWriter;
import java.io.IOException;

import java.util.Scanner;
import java.util.StringTokenizer;

import twitter4j.TwitterStream;



public class WordCount {
	
	static int TweetCount;
	static int JobCount;
	static TwitterStream tweetstream;
	public static String TweetTime;
	public static void main(String [] args) throws Exception {
		
		System.setProperty("hadoop.home.dir", "c:\\winutils\bin");
	
		Scanner  jc = new Scanner(new File("jobcount"));
		JobCount = jc.nextInt();
		jc.close();
		JobCount++;
		addJob(JobCount);
		Scanner sc = new Scanner(new File("tweetcount"));
		TweetCount = sc.nextInt();
		sc.close();
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter keywords");
		String scaninput = input.nextLine();
		
		String[] keywords = scaninput.split("\\s+");
		
		Tweets tweets = new Tweets();
		tweets.streamFeed(keywords);
		System.out.println("type exit to end stream");
		String exitinput = input.nextLine();
		if(exitinput.equals("exit")) {
			System.out.println("Ending stream...");
			input.close();
			
			tweetstream.shutdown();
			
			System.out.println("Starting Hadoop WordCount...");
			Tokenizer.TweetJob();
		}
		
	}
	
	public static void text(String tweettext) {
			
			
			String finalform = tweettext;
			if(!finalform.startsWith("RT")) {
				try {
					finalform = splitText(tweettext);
					TweetCount++;
					ReWrite(TweetCount);
					Tokenizer.WriteTweet(finalform);
					
				}
				catch(Exception e){
					System.out.println("File not found.");
					e.printStackTrace();
					
				}
			System.out.println(finalform + "\r");	
			}			
		}
	

	
	public static void setStream(TwitterStream stream) {
			
			tweetstream = stream;
	}
	
	public static void ReWrite(int count) throws Exception {
		
		String tcount = Integer.toString(count);
		FileWriter fw = new FileWriter("tweetcount", false);
		fw.write(tcount);
		fw.close();
	}
	
	
	
	public static String splitText(String tweettext) {
		
		String regex = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
		StringTokenizer token = new StringTokenizer(tweettext);
		String newtweet = "";
		while(token.hasMoreTokens()) {
			
			String tweet = token.nextToken();
			tweet = tweet.replaceAll(regex, "");
			tweet = tweet.replaceAll("[^a-zA-Z ]", "");
			tweet = tweet.toLowerCase();
			newtweet += " " + tweet;
		}
		
		
	  	
	  
		return newtweet;
		
	}
	
	public static void addJob(int count) throws IOException {
		
		
				
		String jcount = Integer.toString(count);
		FileWriter fwjob = new FileWriter("jobcount", false);
		fwjob.write(jcount);
		fwjob.close();
	}
	
	
}
	


