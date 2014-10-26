package com.bigData.tweetDistiller;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.DBCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class TweetDistiller {
	
	static DBCollection coll;
	public static void mongoDB(){
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}	
		DB db = mongoClient.getDB( "tweetdb" );
		coll = db.getCollection("tvtweets");	
	}
	public void insert(BasicDBObject doc){
			coll.insert(doc);	
	}
	public String timeZoneString(int utc){
		switch (utc){
		case -18000:
			return "Eastern";
		case -21600:
			return "Central";
		case -25200:
			return "Mountain";
		case -28800:
			return "Pacific";
		case -32400:
			return "Alaska";
		case -36000:
			return "Hawaii";
		default:
			return "Non-US";
		}
	}
	
	
	public static void main(String[] args){
		mongoDB();

		
		
		System.out.println("connected");
		BasicDBObject query1 = new BasicDBObject("hashtags.text", "idol");		
		BasicDBObject query2 = new BasicDBObject("hashtags.text", "Idol");
		BasicDBObject query3 = new BasicDBObject("hashtags.text", "americanidol");
		BasicDBObject query4 = new BasicDBObject("hashtags.text", "AmericanIdol");
		BasicDBObject query5 = new BasicDBObject("userMentions.text", "AmericanIdol");
		ArrayList<BasicDBObject> myList = new ArrayList<BasicDBObject>();
		myList.add(query1);
		myList.add(query2);
		myList.add(query3);
		myList.add(query4);
		myList.add(query5);
		long startTime = 1365033600000L;
		long endTime = 1365040800000L;
		long stepDuration = 3000;		
		long stepTime1 = startTime;
		long stepTime2 = stepTime1 + stepDuration;
		int inc = 1;

		try{
			FileWriter statsWriter = new FileWriter("stats.txt", false);
			BufferedWriter statsBuffer = new BufferedWriter(statsWriter);
			FileWriter textWriter = new FileWriter("text.txt", false);
			BufferedWriter textBuffer = new BufferedWriter(textWriter);
		
			while (stepTime2 < endTime){
				stepTime2 = stepTime1 + stepDuration;
				String stats = "";
				String text = "";
				BasicDBObject query = new BasicDBObject("time",
								new BasicDBObject("$gte", stepTime1)
									      .append("$lte", stepTime2))
						.append("utcOffset",
								new BasicDBObject("$gte", -21600)
										  .append("$lte", -18000))
						.append("$or", myList);
				DBCursor cursor = coll.find(query);
	//			System.out.println(cursor.size());
				stats += inc + "," + (stepTime2 - startTime)/1000 + "," + cursor.size()*20;
				while (cursor.hasNext()){
					text += (String) cursor.next().get("content") + "\n";
	//				String content = (String) cursor.next().get("content");
	//				System.out.println(content);
				}
				cursor.close();
				text += "\n********************* " + inc +" *************************\n";
				statsBuffer.write(stats + "\n");
				textBuffer.write(text + "\n");
				stepTime1 = stepTime2;
				inc++;
			}
			statsBuffer.close();
			textBuffer.close();
		}
		
		catch(FileNotFoundException fnf)
		{
			System.out.print("Error: file not found");
		}
		catch(IOException io)
		{
			System.out.print("Error: IO exception");
		}
	}
}






//"AmericanIdol")
//.append("hashtags.text", "americanidol");

//DBObject query = new QueryBuilder()
//.put("time").greaterThanEquals(1365033700000L)
//.put("time").lessThanEquals(1365033703000L)	
//.or(
//QueryBuilder.start("hashtags.text").is("idol").get(),
//QueryBuilder.start("utcOffset").is("americanidol").get()
//)
//.or(
//QueryBuilder.start("utcOffset").is(-18000).get(),
//QueryBuilder.start("utcOffset").is(-21600).get()
//)
//.get();
