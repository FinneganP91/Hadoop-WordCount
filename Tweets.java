
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class Tweets{
	
	
	public void streamFeed(String[] keyword) {
		
		
		
	    StatusListener listener = new StatusListener() {
	 
	        @Override
	        public void onException(Exception e) {
	            e.printStackTrace();
	        }
	        @Override
	        public void onDeletionNotice(StatusDeletionNotice arg) {
	        }
	        @Override
	        public void onScrubGeo(long userId, long upToStatusId) {
	        }
	        @Override
	        public void onStallWarning(StallWarning warning) {
	        }
	        @Override
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	        }
	        @Override
            public void onStatus(final Status status) {
	        	
	        	
                WordCount.text(status.getText());
                

            }
	    
	    };   
	        
	    ConfigurationBuilder config = new ConfigurationBuilder();
		config.setDebugEnabled(true);
		config.setOAuthConsumerKey("****");
		config.setOAuthConsumerSecret("****");
		config.setOAuthAccessToken("****");
		config.setOAuthAccessTokenSecret("****");
				
		TwitterStream twitterStream = new TwitterStreamFactory(config.build()).getInstance();
		
	   
	    FilterQuery fq = new FilterQuery();        

        fq.track(keyword);        
        fq.language("en");
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
        
       
        WordCount.setStream(twitterStream);
	}
	
	public void closeStream(TwitterStream stream) {
		System.out.println("Stream ended, starting Hadoop");
		stream.shutdown();
	}
	
	
        
      
             	
}
	
	
		

