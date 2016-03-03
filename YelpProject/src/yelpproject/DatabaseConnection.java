/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpproject;

//STEP 1. Import required packages
import java.sql.*;

/**
 *
 * @author sumati
 */
public class DatabaseConnection {

    public static void main(String[] argv) {
        try {

            Connection connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521/xe", "system",
                    "sumati1234");

            String sql = "select sysdate as current_day from dual";

            //creating PreparedStatement object to execute query
            PreparedStatement preStatement = connection.prepareStatement(sql);

            ResultSet result = preStatement.executeQuery();

            while (result.next()) {
                System.out.println("Current Date from Oracle : " + result.getString("current_day"));
            }
            System.out.println("done");

        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }
    }

}
