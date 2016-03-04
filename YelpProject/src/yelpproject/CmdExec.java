/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpproject;

/**
 *
 * @author sumati
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.ibatis.common.jdbc.ScriptRunner;

/**
 * 
 @author Dhinakaran Pragasam
 */
public class CmdExec {
	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws ClassNotFoundException,
		SQLException {
		
		String aSQLScriptFilePath = "C:\\Users\\sumati\\Desktop\\createTables.sql";
		
		// Create MySql Connection
		
		Connection con = DriverManager.getConnection(
			"jdbc:oracle:thin:@localhost:1521/xe", "system", "sumati1234");
		Statement stmt = null;

		try {
			// Initialize object for ScripRunner
			ScriptRunner sr = new ScriptRunner(con, false, false);

			// Give the input file to Reader
			Reader reader = new BufferedReader(
                               new FileReader(aSQLScriptFilePath));

			// Exctute script
			sr.runScript(reader);

		} catch (Exception e) {
			System.err.println("Failed to Execute" + aSQLScriptFilePath
					+ " The error is " + e.getMessage());
		}
	}

}
