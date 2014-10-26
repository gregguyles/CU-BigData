package com.bigData.tweetTracker;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class StreamCatcher {
    public static void main(String[] args) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
		// insert credentials below
        cb.setOAuthConsumerKey("");
        cb.setOAuthConsumerSecret("");
        cb.setOAuthAccessToken("");
        cb.setOAuthAccessTokenSecret("");
		
        final MongoDB db = new MongoDB();
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        
        StatusListener listener = new StatusListener() {
            @Override
            public void onException(Exception arg0) {
            	System.out.print("Exception: " + arg0);
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
            	System.out.print("Deletion Notice: " + arg0);
            }
            @Override
            public void onScrubGeo(long arg0, long arg1) {
            	System.out.print("Scrub Geo: " + arg0);
            }
            @Override
            public void onStatus(Status status) {
            	StatusThread st = new StatusThread(status, db);
            	st.run();
            }
            @Override
            public void onTrackLimitationNotice(int arg0) {
            	System.out.print("Track Limitation: " + arg0);
            }
			@Override
			public void onStallWarning(StallWarning arg0) {
				System.out.print("Stall Warning: " + arg0);
			}
        };
        FilterQuery fq = new FilterQuery();
        String keywords[] = {"gameofthrones","gotquotes", "GoTFans",
        					 "madmen", "dondraper"};
        fq.track(keywords);
        twitterStream.addListener(listener);
        twitterStream.filter(fq);  
    }
}


//String keywords[] = {"LoveAndHipHop", "thewalkingdead", "walkingdead", "GirlsHBO",
//		"rhoa", "americanIdol", "idol", "badgirlsclub" , "newgirl", "parksandrec",
//		"bgc", "bgc10", "firsttake", "mobwives", "californication", "hankmoody",
//		"thesisterhood", "wwhl", "thegallaghers", "shameless", "thefollowing"};