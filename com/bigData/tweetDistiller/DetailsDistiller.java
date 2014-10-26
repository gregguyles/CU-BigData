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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

public class DetailsDistiller {
	
	static DBCollection coll;
	public static void mongoDB(){
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
	public static String timeZoneString(long utc){
		switch ((int)utc){
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
		ArrayList<BasicDBObject> myList = new ArrayList<BasicDBObject>();
////////////////////////////////////////////////////
		BasicDBObject query1 = new BasicDBObject("hashtags.text", "gameofthrones");		
		BasicDBObject query2 = new BasicDBObject("hashtags.text", "gotquotes");
		BasicDBObject query3 = new BasicDBObject("hashtags.text", "GoTFans");
		myList.add(query1);
		myList.add(query2);
		myList.add(query3);
		long[][] timeData = new long[][] {{-18000, 1367197200000L},	//eastern
				  {-21600,1367197200000L}};	//central
		long duration = 3600000;//length of program

//////////////////////////////////////////////////////
		DBCursor cursor = null;
		long stepDuration = 3000;	//3sec
		long buffer = 1800000; //30min;
		int inc = 1;
		int utc = 0, start = 1;
		long endTime;
		long showStartTime;
		long week = 604800000; //week
		long day = 86400000; //day 
		long hour = 3600000;
		int h = 0;
		Date date;
		String sDate;
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		try{
			FileWriter statsWriter = new FileWriter("showStats.txt", false);
			BufferedWriter statsBuffer = new BufferedWriter(statsWriter);
			statsBuffer.write("Record_Number,Date,Time_Zone,Relative_Time,Volume,Messages_Scent\n");
			long stepTime1 = 0;
			long stepTime2 = 0;
			long relativeTime;
			for (int w = 0; w < 1; w++) {								// week loop, 1 weeks total									// day loop 
				for (int i = 0; i < timeData.length; i++){				//time zone loop
					showStartTime =  timeData[i][start] - (h * hour)  + (w * week);
					stepTime1 = showStartTime - buffer;
					endTime = (showStartTime + duration) + buffer;
					date = new Date((long)stepTime1);
					sDate = df.format(date);
					while (stepTime1 < endTime){
						stepTime2 = stepTime1 + stepDuration;
						String stats = "";
						BasicDBObject query = new BasicDBObject("time",
										new BasicDBObject("$gte", stepTime1)
											      .append("$lte", stepTime2))
								.append("utcOffset",timeData[i][utc])
								.append("$or", myList);
						cursor = coll.find(query);
						int size = cursor.size();
						int sumFollowers = 0;
						while(cursor.hasNext()){
							sumFollowers += Integer.parseInt(cursor.next().get("followers").toString());
						}
						if (stepTime2 >= showStartTime)
							relativeTime = (stepTime2 - showStartTime)/60000;
						else
							relativeTime = -(showStartTime - stepTime2)/60000;
						stats +=  inc + ","
								+ sDate + ","
								+ timeZoneString(timeData[i][utc]) + "," 
								+ relativeTime + "," 
								+ size*20 + ","
								+ sumFollowers;
						statsBuffer.write(stats + "\n");
						stepTime1 = stepTime2;
						inc++;
					}
				}
				if (w == 2)
					h = 1;
			}
		cursor.close();
		statsBuffer.close();
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
//walking Dead
/*
BasicDBObject query1 = new BasicDBObject("hashtags.text", "walkingdead");		
BasicDBObject query2 = new BasicDBObject("hashtags.text", "WalkingDead");
BasicDBObject query3 = new BasicDBObject("hashtags.text", "thewalkingdead");
BasicDBObject query4 = new BasicDBObject("hashtags.text", "TheWalkingDead");
BasicDBObject query5 = new BasicDBObject("userMentions.text", "@WalkingDead_AMC");

long[][] timeData = new long[][] {{-18000,1361152800000L},	//eastern
		  {-21600,1361152800000L},	//central
		  {-25200,1361163600000L},	//mountain
		  {-28800,1361163600000L},	//pacific
		  {-32400,1361163600000L},	//Alaska
		  {-36000,1361160000000L}}; //Hawaii
long duration = 3600000;//length of program
*/

//Shameless
/*
BasicDBObject query1 = new BasicDBObject("hashtags.text", "Shameless");		
BasicDBObject query2 = new BasicDBObject("hashtags.text", "shameless");
BasicDBObject query3 = new BasicDBObject("hashtags.text", "TeamGallagher");
BasicDBObject query4 = new BasicDBObject("hashtags.text", "teamgallagher");
BasicDBObject query5 = new BasicDBObject("hashtags.text", "thegallaghers");
BasicDBObject query6 = new BasicDBObject("hashtags.text", "TheGallaghers");
BasicDBObject query7 = new BasicDBObject("userMentions.text", "@SHO_Shameless");
myList.add(query1);
myList.add(query2);
myList.add(query3);
myList.add(query4);
myList.add(query5);
myList.add(query6);
myList.add(query7);

long[][] timeData = new long[][] {{-18000,1361152800000L},	//eastern
		  {-21600,1361757600000L},	//central
		  {-25200,1361757600000L}};	//mountain
long duration = 3600000;//length of program
*/

//Californication
/*
BasicDBObject query1 = new BasicDBObject("hashtags.text", "californication");		
BasicDBObject query2 = new BasicDBObject("hashtags.text", "Californication");
BasicDBObject query3 = new BasicDBObject("hashtags.text", "hankmoody");
BasicDBObject query4 = new BasicDBObject("hashtags.text", "HankMoody");
BasicDBObject query5 = new BasicDBObject("userMentions.text", "Californication");
myList.add(query1);
myList.add(query2);
myList.add(query3);
myList.add(query4);
myList.add(query5);
long[][] timeData = new long[][] {{-18000,1361152800000L},	//eastern
		  {-21600,1361763000000L}};	//central
long duration = 1800000;//length of program
*/

