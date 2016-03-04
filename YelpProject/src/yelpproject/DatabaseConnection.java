/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpproject;

//STEP 1. Import required packages
import java.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author sumati
 */
public class DatabaseConnection {

    private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_CONNECTION = "jdbc:oracle:thin:@localhost:1521/xe";
    private static final String DB_USER = "system";
    private static final String DB_PASSWORD = "su*******4";
    public static Connection connection;

    public static void main(String[] argv) {

        DatabaseConnection dc = new DatabaseConnection();
        try {

            dc.dropTable();
            dc.createTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable() throws SQLException {

        Statement stmt1 = null;

        String sql1 = "create table "
                + "REVIEW(votes varchar(100),user_id varchar(30),review_id varchar(30) "
                + "PRIMARY KEY, stars number(10), type varchar(10), text clob, "
                + "business_id varchar(30),date_review DATE)";

        String sql2 = "create table "
                + "CHECKIN(business_id VARCHAR(1000), type VARCHAR(100), checkin_day NUMBER(10), checkin_hour NUMBER(10), "
                + "checkin_value NUMBER(10))";

        String sql3 = "create table "
                + "YELP_USER "
                + "(yelping_since DATE,votes VARCHAR(100),name VARCHAR(50), review_count NUMBER(20),"
                + "user_id VARCHAR(30) PRIMARY KEY, friends NUMBER(10),"
                + "fans NUMBER(20),average_stars NUMBER(20),type VARCHAR(10),compliments VARCHAR(1000),elite VARCHAR(100))";

        String sql5 = "create table BUSINESS "
                + "(business_id VARCHAR(1000) PRIMARY KEY,"
                + "categories VARCHAR(3999),review_count NUMBER(20),"
                + "name VARCHAR(100),stars VARCHAR(20))";

        String sql6 = "create table "
                + "BUSINESS_CATEGORY "
                + "(business_category VARCHAR(100))";

        String sql7 = "create table BUSINESS_SUBCATEGORY"
                + "(business_subcategory VARCHAR(200))";

        try {
            connection = getDBConnection();

            DatabaseMetaData dbm1 = connection.getMetaData();
            ResultSet rs1 = dbm1.getTables(null, null, "REVIEW", null);
            if (rs1.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created REVIEW");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql1);
            }

            DatabaseMetaData dbm2 = connection.getMetaData();
            ResultSet rs2 = dbm2.getTables(null, null, "CHECKIN", null);
            if (rs2.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created. CHECKIN");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql2);
            }

            DatabaseMetaData dbm3 = connection.getMetaData();
            ResultSet rs3 = dbm3.getTables(null, null, "YELP_USER", null);
            if (rs3.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created." + " Yelp user");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql3);
            }

            DatabaseMetaData dbm5 = connection.getMetaData();
            ResultSet rs5 = dbm5.getTables(null, null, "BUSINESS", null);
            if (rs5.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created. BUSINESS");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql5);
            }

            DatabaseMetaData dbm6 = connection.getMetaData();
            ResultSet rs6 = dbm6.getTables(null, null, "BUSINESS_CATEGORY", null);
            if (rs6.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created. BUSINESS_CATEGORY");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql6);
            }

            DatabaseMetaData dbm7 = connection.getMetaData();
            ResultSet rs7 = dbm7.getTables(null, null, "BUSINESS_SUBCATEGORY", null);
            if (rs7.next()) {
                System.out.println("Table already exists. Not creating.");
                //stmt = connection.createStatement();
                //stmt.executeUpdate(sql2);
            } else {
                System.out.println("Table created. BUSINESS_SUBCATEGORY");
                stmt1 = connection.createStatement();
                stmt1.executeUpdate(sql7);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt1 != null) {
                stmt1.close();
            }
        }
    }

    public static void insertIntoYelpUserTable(java.sql.Date yelping_since,
            JSONObject votes,
            long review_count,
            String name,
            String user_id,
            int friends,
            long fans,
            double average_stars,
            String type,
            JSONObject compliments,
            JSONArray elite) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO YELP_USER"
                + " VALUES(?,?,?,?,?,?,?,?,?,?,?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            preparedStatement.setDate(1, yelping_since);
            preparedStatement.setString(2, String.valueOf(votes));
            preparedStatement.setString(3, name);
            preparedStatement.setLong(4, review_count);
            preparedStatement.setString(5, user_id);
            preparedStatement.setInt(6, friends);
            preparedStatement.setLong(7, fans);
            preparedStatement.setDouble(8, average_stars);
            preparedStatement.setString(9, type);
            preparedStatement.setString(10, String.valueOf(compliments));
            preparedStatement.setString(11, String.valueOf(elite));

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into YELP_USER table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    public static void insertIntoReviewTable(
            JSONObject votes,
            String user_id,
            String review_id,
            long stars,
            java.sql.Date date,
            String text,
            String type,
            String business_id) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO REVIEW"
                + " VALUES(?,?,?,?,?,?,?,?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            //
            preparedStatement.setString(1, String.valueOf(votes));
            preparedStatement.setString(2, user_id);
            preparedStatement.setString(3, review_id);
            preparedStatement.setLong(4, stars);
            preparedStatement.setString(6, text);
            preparedStatement.setString(5, type);

            preparedStatement.setString(7, business_id);
            preparedStatement.setDate(8, date);

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into REVIEW table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    public static void insertIntoCheckinTable(
            String business_id,
            String type,
            int checkin_day,
            int checkin_hour,
            long checkin_value) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO CHECKIN"
                + " VALUES(?,?,?,?,?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            preparedStatement.setString(1, business_id);
            preparedStatement.setString(2, type);
            preparedStatement.setInt(3, checkin_day);
            preparedStatement.setInt(4, checkin_hour);
            preparedStatement.setLong(5, checkin_value);

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into CHECKIN table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    public static void insertIntoBusinessTable(
            String business_id,
            String categories,
            long review_count,
            String name,
            double stars) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO BUSINESS"
                + " VALUES(?,?,?,?,?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            preparedStatement.setString(1, business_id);
            preparedStatement.setString(2, categories);
            preparedStatement.setLong(3, review_count);
            preparedStatement.setString(4, name);
            preparedStatement.setDouble(5, stars);

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into BUSINESS table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    public static void insertIntoBusinessCategoryTable(
            String business_category) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO BUSINESS_CATEGORY"
                + " VALUES(?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            preparedStatement.setString(1, business_category);

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into BUSINESS_CATEGORY table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    public static void insertIntoBusinessSubCategoryTable(
            String business_subcategory) throws SQLException {

        PreparedStatement preparedStatement = null;

        String insertTableSQL = "INSERT INTO BUSINESS_SUBCATEGORY"
                + " VALUES(?)";

        try {

            if (connection == null) {
                connection = getDBConnection();
            }
            preparedStatement = connection.prepareStatement(insertTableSQL);

            preparedStatement.setString(1, business_subcategory);

            // execute insert SQL stetement
            preparedStatement.executeUpdate();

            System.out.println("Record is inserted into BUSINESS_SUBCATEGORY table!");

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
       
    private static Connection getDBConnection() {

        Connection dbConnection = null;

        try {

            Class.forName(DB_DRIVER);

        } catch (ClassNotFoundException e) {

            System.out.println(e.getMessage());

        }

        try {

            dbConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        }

        return dbConnection;

    }

    private static java.sql.Timestamp getCurrentTimeStamp() {

        java.util.Date today = new java.util.Date();
        return new java.sql.Timestamp(today.getTime());

    }

    private void dropTable() throws SQLException {

        Statement stmt = null;

        String sqlQuery = "drop table REVIEW";

        try {
            connection = getDBConnection();

            DatabaseMetaData dbm1 = connection.getMetaData();
            ResultSet rs1 = dbm1.getTables(null, null, "REVIEW  ", null);
            if (rs1.next()) {
                stmt = connection.createStatement();
                stmt.executeUpdate(sqlQuery);
                System.out.println("Dropped!");
            } else {
                System.out.println("Table does not exist");

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

    }

}
