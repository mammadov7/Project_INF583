package inf583.wordcount_sparksql;

import java.util.Scanner;
import java.util.*;

import java.io.File;
import java.io.IOException;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

//private static <T> ClassTag<T> classTag(Class<T> clazz) 
//{
//	return scala.reflect.ClassManifestFactory.fromClass(clazz);
//}
//


public class ApacheSparkSQL {

	public static void main(String[] args) throws AnalysisException {

	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
	
	SparkSession spark = SparkSession.builder().appName("Java Spark SQL for Twitter").config("spark.master", "local[*]").getOrCreate();
	SQLContext sqlContext = new SQLContext(spark);
	SparkContext sc = spark.sparkContext();
	
	// Reading a JSON file
	Dataset<Row> tweets = spark.read().json("datasets/2020-02-01.json");
	Dataset<Row> stopWords = spark.read().json("datasets/2020-02-01.json");

//	Scanner inFile1;
//    try {
//		Scanner inFile1 = new Scanner(new File("datasets/stop_words_french.json")).useDelimiter(",");
//    } catch (IOException e) {
//        e.printStackTrace(); 
//    }
	
//	List<String> temps = new ArrayList<String>();
//	String token1 = "";
	
//	while(inFile1.hasNext())
//	{
//		token1 = inFile1.next();
//		temps.add(token1);
//	}
//	inFile1.close();
	
//	String[] tempsArray = temps.toArray(new String[0]);
	
	//Broadcast<String[]> broadcastVar = sc.broadcast(tempsArray, classTag(tempsArray.class));
	// Displays the content of the Dataset to stdout
//	tweets.show(5);
	StructType schema = new StructType()
	.add("userID", "int")
	.add("movieID", "int")
	.add("rating", "float")
	.add("timestamp", "long");

	tweets.show(5);
	tweets.select("text").show(5);
	
	
	// Creating a new Dataset by adding the column words
	Dataset<Row> tweetsWords = tweets.withColumn("words", functions.split(functions.lower(tweets.col("text")), " |,"));//|.|!|:|;|[|]|/|\\?|-
    tweetsWords.show(5);

	Dataset<Row> words = tweetsWords.withColumn("words_separated", functions.explode(tweetsWords.col("words"))).select("words_separated");
    words.show(5);
    
    // Removing blank words and stop words
    Dataset<Row> words_filtered = words.filter("words_separated != ''");
    								   //.filter(!functions.array_contains(col("words_separated"), broadcastVar));
    
    Dataset<Row> counts = words_filtered.groupBy("words_separated").count();
    counts.show(5);
    
    words_filtered.createGlobalTempView("words");
	spark.sql("SELECT words_separated, count(words_separated) as occurences FROM global_temp.words GROUP BY words.words_separated ORDER BY occurences DESC").show(5);;

	
	// Reading a CSV file
	//Dataset<Row> ratings = spark.read().option("header", "true").schema(schema).csv("datasets/ratings.csv");
	// Displays the content of the Dataset to stdout
//	ratings.show(5);


//	tweets.filter(col("coordinates").isNotNull()).select("coordinates").where(  ).show(5, false);

//	// Select only the "text" column
//	tweets.select("text").show(5, false);
//
//    // Select tweets from users that enabled geolocation
//	tweets.filter(col("place").isNotNull()).select("place").show(5, false);

	// Exercise 1

//	tweets.createGlobalTempView("tweets");
//	spark.sql("SELECT coordinates, place FROM global_temp.tweets WHERE place.country LIKE '%United States%'").show(10, false);

	// Exercise 2
//	spark.sql("SELECT COUNT(*) as nb_retweets FROM global_temp.tweets WHERE retweeted_status IS NOT NULL").show();
//	System.out.println(tweets.filter(col("retweeted_status").isNotNull()).count());

	//ratings.createGlobalTempView("ratings");
	// Exercise 3
//	spark.sql("SELECT userID, COUNT(movieID) FROM global_temp.ratings GROUP BY userID ORDER BY userID ").show(100, false);

	// Exercise 4
	// spark.sql("SELECT movieID, SUM(rating)/COUNT(movieID) Average " +
	//				 "FROM global_temp.ratings " +
	//		   		 "GROUP BY movieID ORDER BY movieID").show(100, false);
	// For checking
	// awk 'BEGIN {FS = ","} ; {if($2 == "2"){ sum+=$3; count+=1 }} END {print sum/ count}' ratings.csv

	// Exercise 5
//		spark.sql("SELECT t1.userID, t2.userID " +
//				"FROM global_temp.ratings t1, global_temp.ratings t2 ").show(100, false);



//	// Register the Dataset as a global temporary view

//
//	// Global temporary view is tied to a system preserved database `global_temp`
//	// We show the same operations as above but now expressed using SQL
//	spark.sql("SELECT * FROM global_temp.tweets").show(5);
//	spark.sql("SELECT text FROM global_temp.tweets").show(5);
//	spark.sql("SELECT coordinates FROM global_temp.tweets WHERE coordinates IS NOT NULL").show(5, false);
//	spark.sql("SELECT place.name FROM global_temp.tweets WHERE place IS NOT NULL").show(5, false);
//
//
//	// Creating a new Dataset by adding the column words
//	Dataset<Row> tweetsWords = tweets.withColumn("words", functions.split(tweets.col("text"), " "));
//    tweetsWords.show(5);
//
//	Dataset<Row> words = tweetsWords.withColumn("words_separated", functions.explode(tweetsWords.col("words"))).select("words_separated");
//    words.show(5);
//
//    Dataset<Row> counts = words.groupBy("words_separated").count();
//    counts.show(5);
//
//    words.createGlobalTempView("words");
//	spark.sql("SELECT words_separated, count(words_separated) FROM global_temp.words GROUP BY words.words_separated").show(5);;
   }
}