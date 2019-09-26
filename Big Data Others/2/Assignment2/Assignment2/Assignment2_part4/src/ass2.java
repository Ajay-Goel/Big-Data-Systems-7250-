
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.bson.Document;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ajaygoel
 */
public class ass2 {

    public static void main(String args[]) throws UnknownHostException, FileNotFoundException, IOException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        MongoDatabase database = mongo.getDatabase("bigdata_Ass2");
//        
        database.createCollection("accessLogs3");
        //System.out.println("Collection accessLogs created successfully");
        String file = "/Users/ajaygoel/Documents/Neu/BigData/Assignment2/access.log.txt";
        String line = "";
        String SplitBy2 = " - - ";
        MongoCollection<Document> collection4 = database.getCollection("accessLogs3");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(SplitBy2);
                String str = tokens[1];
                String time = str.substring(str.indexOf("[") + 1, str.indexOf("]"));
                //System.out.println(time);
                String request = str.substring(str.indexOf("\"") + 1);
                String[] tre = request.split(" ", 2);
                String[] tre2 = request.split(" ");
                //String times[] = time.split(" ",2);
                //times[0]=times[0].replaceAll("/","-");
                time = time.replaceFirst(":", " ");
                //System.out.println(time);
                //times[0]=times[0].replaceFirst(":"," ");
                //System.out.println(times[0]);
                //System.out.println(times[1]);

                try {
                    Document doc = new Document("IpAddress", tokens[0])
                            .append("TimeStamp", time)
                            .append("Request", tre[0])
                            .append("Code", tre2[3]);
                    collection4.insertOne(doc);
                } catch (Exception e) {
                    Document doc = new Document("IpAddress", tokens[0])
                            .append("TimeStamp", time)
                            .append("Request", tre[0])
                            .append("Code", "not_found");
                    collection4.insertOne(doc);
                }

            }
            br.close();
        }
        System.out.println("Successfully completed file logs");

        database.createCollection("movies2");
        System.out.println("Collection created successfully");
        MongoCollection<Document> collection = database.getCollection("movies2");

        String file1 = "/Users/ajaygoel/Documents/Neu/BigData/Assignment2/ml-1m/movies.dat";
        String SplitBy = "::";
        try (BufferedReader br = new BufferedReader(new FileReader(file1))) {
            while ((line = br.readLine()) != null) {
                String[] movies = line.split(SplitBy);
                //System.out.println(movies[0] +  "=>" + movies[1]+  "=>" + movies[2]);

                //fetching id
                //System.out.println(movies[0]);
                //fetching year
                String str = movies[1];
                String year = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
                //System.out.println(answer);

                //fetching genres
                String[] Genres = movies[2].split("\\|", 0);
                //Document doc_genres = new Document();
                //for(String g:Genres)
                //  doc_genres.append("Genre", g);
                //System.out.println(line);
                Document doc = new Document("movieID", movies[0])
                        .append("title", movies[1])
                        .append("year", year);
                //.append("genre", Genres);
                //.append("Genres", movies[2]);
                doc.put("Genre", Arrays.asList(Genres));
                collection.insertOne(doc);
            }
            br.close();
        }
        System.out.println("Successfully completed file 1 - movies");

        database.createCollection("ratings");
        System.out.println("Collection created successfully");

        MongoCollection<Document> collection2 = database.getCollection("ratings");

        String file2 = "/Users/ajaygoel/Documents/Neu/BigData/Assignment2/ml-1m/ratings.dat";
        try (BufferedReader br = new BufferedReader(new FileReader(file2))) {
            while ((line = br.readLine()) != null) {
                String[] ratings = line.split(SplitBy);
                Document doc = new Document("userId", ratings[0])
                        .append("movieId", ratings[1])
                        .append("rating", ratings[2])
                        .append("timeStamp", ratings[3]);
                collection2.insertOne(doc);
            }
            br.close();
        }
        System.out.println("Successfully completed file 2 - ratings");

        database.createCollection("tags");
        System.out.println("Collection created successfully");

        MongoCollection<Document> collection3 = database.getCollection("tags");

        String file3 = "/Users/ajaygoel/Documents/Neu/BigData/Assignment2/ml-1m/users.dat";
        try (BufferedReader br = new BufferedReader(new FileReader(file3))) {
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(SplitBy);
                Document doc = new Document("UserID", tokens[0])
                        .append("Gender", tokens[1])
                        .append("Age", tokens[2])
                        .append("Occupation", tokens[3])
                        .append("Zip-code", tokens[4]);
                collection3.insertOne(doc);
            }
            br.close();
        }
        System.out.println("Successfully completed file 3 - tags");
    }

}
