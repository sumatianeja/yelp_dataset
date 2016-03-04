package yelpproject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MyJsonParser {

    public HashSet<String> mainCategories = new HashSet(Arrays.asList("Active Life", "Arts & Entertainment", "Automotive",
            "Car Rental", "Cafes",
            "Beauty & Spas", "Convenience Stores", "Detists", "Doctors", "Drugstores", "Department Stores",
            "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical",
            "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers",
            "Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping", "Transportation"));

    public static void main(String[] args) {
        MyJsonParser parser = new MyJsonParser();
        parser.readJsonFileReview();
    }

    public void readJsonFileYelpUser() {

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

                    
                  
                    java.util.Date utilDate = null;
                    try {
                        utilDate = (java.util.Date) new SimpleDateFormat("yyyy-MM").parse(yelping_since);
                    } catch (java.text.ParseException ex) {
                        Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    // because PreparedStatement#setDate(..) expects a java.sql.Date argument
                    java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
                    try {
                        DatabaseConnection.insertIntoYelpUserTable(sqlDate, votes, review_count, name, user_id, friends.size(), fans, average_stars, type, compliments, elite);
                    } catch (SQLException ex) {
                        Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                    }

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            System.out.println(countLines);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void readJsonFileReview() {

        BufferedReader br = null;
        JSONParser parser = new JSONParser();

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("C:\\Users\\sumati\\Desktop\\YelpDataset-CptS451\\yelp_review.json"));

            int countLines = 1;

            //{"votes": {"funny": 6, "useful": 0, "cool": 0}, "user_id": "ZYaumz29bl9qHpu-KVtMGA", 
            //"review_id": "ow1c4Lcl3ObWxDC2yurwjQ", "stars": 4, "date": "2009-05-04", 
            //"text": "If you like lot lizards, you'll love the Pine Cone!", "type": "review", "business_id": "JwUE5GmEO-sH1FuwJgKBlQ"}
            while ((sCurrentLine = br.readLine()) != null) {

                Object obj;
                try {
                    obj = parser.parse(sCurrentLine);
                    JSONObject jsonObject = (JSONObject) obj;

                    JSONObject votes = (JSONObject) jsonObject.get("votes");
                    String user_id = (String) jsonObject.get("user_id");
                    String review_id = (String) jsonObject.get("review_id");
                    long stars = (Long) jsonObject.get("stars");
                    String date = (String) jsonObject.get("date");
                    String text = (String) jsonObject.get("text");
                    String type = (String) jsonObject.get("type");
                    String business_id = (String) jsonObject.get("business_id");
                    countLines++;

                    java.util.Date utilDate = null;
                    try {
                        utilDate = (java.util.Date) new SimpleDateFormat("YYYY-MM-DD").parse(date);
                    } catch (java.text.ParseException ex) {

                        Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    // because PreparedStatement#setDate(..) expects a java.sql.Date argument
                    java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
                    try {
                        DatabaseConnection.insertIntoReviewTable(votes, user_id, review_id, stars, sqlDate, text, type, business_id);
                    } catch (SQLException ex) {
                        Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                    }

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            System.out.println(countLines);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void readJsonFileCheckin() {

        BufferedReader br = null;
        JSONParser parser = new JSONParser();

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("C:\\Users\\sumati\\Desktop\\YelpDataset-CptS451\\yelp_checkin.json"));

            //{"checkin_info": {"11-5": 1, "11-4": 1, "19-1": 1, "10-6": 2}, 
            //"type": "checkin", "business_id": "a933ecROIB44zhzos11YTg"}
            while ((sCurrentLine = br.readLine()) != null) {

                Object obj;
                try {
                    obj = parser.parse(sCurrentLine);
                    JSONObject jsonObject = (JSONObject) obj;

                    JSONObject checkin_info = (JSONObject) jsonObject.get("checkin_info");
                    String type = (String) jsonObject.get("type");
                    String business_id = (String) jsonObject.get("business_id");

                    //HashMap<String, Integer> map = new HashMap<String, Integer>();
                    Iterator<?> entries = checkin_info.entrySet().iterator();

                    while (entries.hasNext()) {
                        Entry entry = (Entry) entries.next();
                        String key = (String) entry.getKey();
                        Long value = (Long) entry.getValue();
                        String[] arr = key.split("-");
                        int hour = Integer.parseInt(arr[0]);
                        int dayInt = Integer.parseInt(arr[1]);
                        try {
                            DatabaseConnection.insertIntoCheckinTable(business_id, type, dayInt, hour, value);
                        } catch (SQLException ex) {
                            Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void readJsonFileBusiness() {

        BufferedReader br = null;
        JSONParser parser = new JSONParser();

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("C:\\Users\\sumati\\Desktop\\YelpDataset-CptS451\\yelp_business.json"));

            //{"business_id": "JTiqc8D0MJpYbnObHuNA9w", "full_address": "1331 N 7th St\nPhoenix, AZ 85006", 
            //"hours": {}, "open": false, "categories": ["Doctors", "Health & Medical"], 
            //"city": "Phoenix", "review_count": 3, "name": "Center For Spinal Disorders", "neighborhoods": [], 
            //"longitude": -112.064401, "state": "AZ", "stars": 2.5, "latitude": 33.463512000000001, 
            //"attributes": {"By Appointment Only": true}, "type": "business"}
            while ((sCurrentLine = br.readLine()) != null) {

                Object obj;
                try {
                    obj = parser.parse(sCurrentLine);
                    JSONObject jsonObject = (JSONObject) obj;

                    String business_id = (String) jsonObject.get("business_id");
                    JSONArray categoryArray = (JSONArray) jsonObject.get("categories");
                    long review_count = (long) jsonObject.get("review_count");
                    String name = (String) jsonObject.get("name");
                    double stars = (double) jsonObject.get("stars");

                    String element = null;
                    for (int i = 0; i < categoryArray.size(); i++) {
                        if (!mainCategories.contains(element)) {
                            try {
                                DatabaseConnection.insertIntoBusinessSubCategoryTable(element);
                            } catch (SQLException ex) {
                                Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            element = element + " " + (String) categoryArray.get(i);
                        }
                    }

                    try {
                        DatabaseConnection.insertIntoBusinessTable(business_id, element, review_count, name, stars);
                    } catch (SQLException ex) {
                        Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
                    }

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void readJsonFileBusinessCategory() {

        Iterator i = mainCategories.iterator();
        while (i.hasNext()) {
            String element = (String) i.next();
            try {
                DatabaseConnection.insertIntoBusinessSubCategoryTable(element);
            } catch (SQLException ex) {
                Logger.getLogger(MyJsonParser.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
