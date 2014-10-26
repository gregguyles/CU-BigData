package com.bigData.tweetTracker;

import java.util.ArrayList;
import com.mongodb.BasicDBObject;
import twitter4j.Status;
import twitter4j.HashtagEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import com.bigData.tweetTracker.MongoDB;

public class StatusThread implements Runnable{
	Status status;
	MongoDB db;
	public StatusThread(Status status, MongoDB db) {
		this.status = status;
		this.db = db;
	}

	@Override
	public void run() {
		User user = status.getUser();
    	ArrayList<BasicDBObject> a_Hashtags = new ArrayList<BasicDBObject>();
    	ArrayList<BasicDBObject> a_UserMentions = new ArrayList<BasicDBObject>();				

    	HashtagEntity[] hashtags = status.getHashtagEntities();
    	for (int i = 0; i < hashtags.length; i++){
    		a_Hashtags.add( new BasicDBObject("text", hashtags[i].getText()));
    	}
    	UserMentionEntity[] userMentions = status.getUserMentionEntities();
    	for (int i = 0; i < userMentions.length; i++){
    		a_UserMentions.add( new BasicDBObject("text", userMentions[i].getScreenName()));
    	}
    	BasicDBObject doc = new BasicDBObject("tweetID", status.getId()).
    			append("retweet", status.getRetweetCount()).
    			append("username", user.getScreenName()).
    			append("followers", (long)user.getFollowersCount()).
    			append("time", status.getCreatedAt().getTime() ).
    			append("utcOffset", (long)user.getUtcOffset()).
    			append("timeZone", user.getTimeZone()).
    			append("profileLocation", user.getLocation()).
    			append("hashtags", a_Hashtags).
    			append("userMentions", a_UserMentions).
    			append("content", status.getText());
    	db.insert(doc);
	}
}
