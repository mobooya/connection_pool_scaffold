package com.opower.connectionpool;
import org.junit.*;
import org.junit.Assert.*;
import java.util.*;
import java.sql.*;

public class Test1{

  @Test	
  public void testGetDB() {

    myConnectionPool mypool = null;
    try {
      System.out.println("Hello, yes this is Test");
      //driver - use mysql jdbc driver= com.mysql.jdc.Driver
      //url to my mysql addressbook database= jdbc:mysql://localhost/addressbook
      mypool = new myConnectionPool	("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/addressbook",
					 "root", "131690Mo", 5,8, true);
    }
    catch (SQLException se) {
      System.out.println("ConnectionPool failed, Check output console for stack trace");
      se.printStackTrace();
      return;
    }

    Connection connection = null;
    try {
      connection = mypool.getConnection();
    }
    catch (SQLException se) {
      System.out.println("getConnection failed, Check output console for stack trace");
      se.printStackTrace();
      return;
    }
    System.out.println("We obtained a connection, lets show tables in AddressBook DB");
 
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      String _listDB = "SHOW TABLES IN ADDRESSBOOK";
      rs = stmt.executeQuery(_listDB);
      while (rs.next()) {
        String table = rs.getString(1); //should return name of the table
	System.out.println("Table: " + table);
	Assert.assertTrue(table.equals("contacts") || table.equals("schema_migrations"));
      }
    }
    catch (SQLException se) {
      System.out.println("SQLException: " + se.getMessage());
      System.out.println("SQLException: " + se.getSQLState());
      System.out.println("SQLException: " + se.getErrorCode());
    }
    finally {
      if (rs != null) {
	try { rs.close(); }
	catch (SQLException e) {} //ignore)
      } //if rs not null
      if (stmt != null) {
        try { stmt.close(); }
 	catch (SQLException e) {} //ignore)
      } //if stmt not null
    } //finally

    //Release the connection
    try {
      mypool.releaseConnection(connection);
    }
    catch (SQLException se) {
      System.out.println("SQLException: " + se.getMessage());
      System.out.println("SQLException: " + se.getSQLState());
      System.out.println("SQLException: " + se.getErrorCode());
    }

    //Close the pool
    try {
      mypool.closeAllConnections();
    } catch (Exception e) {
      System.out.println("Could not close all connections in Connectionpool, Exception: ");
      e.printStackTrace();
    }
  } // public void getDB 

} //public class Test1



