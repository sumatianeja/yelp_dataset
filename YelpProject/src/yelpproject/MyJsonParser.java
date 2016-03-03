 package yelpproject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MyJsonParser
{
    public static void main(String[] args)
    {        
        readJsonFile();
    }

    public static void readJsonFile() {

        BufferedReader br = null;
        JSONParser parser = new JSONParser();

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("C:\\Users\\sumati\\Desktop\\YelpDataset-CptS451\\yelp_user.json"));
             
            int countLines = 0;
            
            //"yelping_since": "2012-02", "votes": {"funny": 1, "useful": 5, "cool": 0}, "review_count": 6, 
               //"name": "Lee", "user_id": "qtrmBGNqCvupHMHL_bKFgQ", "friends": [], "fans": 0, 
               //"average_stars": 3.8300000000000001, "type": "user", "compliments": {}, "elite": []}
            while ((sCurrentLine = br.readLine()) != null) {
                
                Object obj;
                try {
                    obj = parser.parse(sCurrentLine);
                    JSONObject jsonObject = (JSONObject) obj;

                    String yelping_since = (String) jsonObject.get("yelping_since");
                    JSONObject votes = (JSONObject) jsonObject.get("votes");
                    long funny = (Long) votes.get("funny");
                    Object useful = votes.get("useful");
                    Object cool =  votes.get("cool");
                    long review_count = (Long) jsonObject.get("review_count");
                    String name = (String) jsonObject.get("name");
                    String user_id = (String) jsonObject.get("user_id");
                    JSONArray friends = (JSONArray) jsonObject.get("friends");
                    long fans = (Long) jsonObject.get("fans");
                    double average_stars = (Double) jsonObject.get("average_stars");
                    String type = (String) jsonObject.get("type");
                    JSONObject compliments = (JSONObject) jsonObject.get("compliments");
                    JSONArray elite = (JSONArray) jsonObject.get("elite");
                    countLines++;
                    System.out.println(countLines);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
