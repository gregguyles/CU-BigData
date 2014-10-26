package com.bigData.tweetTracker;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import java.net.UnknownHostException;

public class MongoDB {
	
	static DBCollection coll;
	public MongoDB(){
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}	
		DB db = mongoClient.getDB( "tweetdb2" );
		coll = db.getCollection("tvtweets2");	
	}
	public void insert(BasicDBObject doc){
			coll.insert(doc);		
	}
}
