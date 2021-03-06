package com.opower.connectionpool;
import org.junit.*;
import org.junit.Assert.*;
import java.util.*;
import java.sql.*;

public class Test2{

  public static final int MAXCON = 8; //Maximum connections used for pool1
  public static final int INITCON = 5; //Initital connections used for pool1
  public static final int MAXCON2 = 1; //Max connections used for pool2
  public static final int INITCON2 = 1; //Initital connections used for pool2
  public static final String DRIVER = "com.mysql.jdbc.Driver";
  public static final String URL = "jdbc:mysql://localhost/addressbook";
  private static final String USER = "root";
  private static final String PASSWORD = "131690Mo";
 
  private myConnectionPool mypool; //this pool waits if its full
  private myConnectionPool mypoolDoesntWait; //this one doesnt wait, says Cant make conncetion
  private Vector<Connection> connectionArray;
  private Vector<Connection> connectionArray2;
  private Statement stmt;
  private ResultSet rs;

  @Before
  public void setUp() {
 
    try {
      System.out.println("Hello, yes this is Setup Test2");
      //driver - use mysql jdbc driver= com.mysql.jdc.Driver
      //url to my mysql addressbook database= jdbc:mysql://localhost/addressbook
      mypool = new myConnectionPool	(DRIVER, URL,
					 USER, PASSWORD, INITCON,MAXCON, true);
      mypoolDoesntWait = new myConnectionPool	(DRIVER, URL,
					 USER, PASSWORD, INITCON2,MAXCON2, false);
 
      connectionArray = new Vector<Connection>(MAXCON);
      connectionArray2 = new Vector<Connection>(MAXCON2);
    
      for (int i =0; i < INITCON; i++) {
      	Connection connection = mypool.getConnection();
	connectionArray.addElement(connection);
      }
      for (int i=0; i < INITCON2; i++){
	Connection connection = mypoolDoesntWait.getConnection();
	connectionArray2.addElement(connection);
      }
	

    }
    catch (SQLException se) {
      System.out.println("ConnectionPool failed, Check output console for stack trace");
      se.printStackTrace();
      return;
    }

  }

  @After
  public void tearDown() {
    System.out.println("TearDowm Test2");
 
     if (rs != null) {
	try { rs.close(); }
	catch (SQLException e) {} //ignore)
      } //if rs not null
      if (stmt != null) {
        try { stmt.close(); }
 	catch (SQLException e) {} //ignore)
      } //if stmt not null

      connectionArray.clear();
      connectionArray2.clear();
      mypool.closeAllConnections();
      mypoolDoesntWait.closeAllConnections();
  }

  @Test	
  public void testGetDB() {
 
    System.out.println("Case1: Lets show tables in AddressBook DB");
    //Setup already got 5 connections, so use one
    Connection myconn = connectionArray.get(0); 

    try {
      stmt = myconn.createStatement();
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

    //Release a connection
    try {
      mypool.releaseConnection(myconn);
    }
    catch (SQLException se) {
      System.out.println("SQLException: " + se.getMessage());
      System.out.println("SQLException: " + se.getSQLState());
      System.out.println("SQLException: " + se.getErrorCode());
    }
  } // public void getDB 


  @Test	
  public void testMakeBGConn() {
 
    System.out.println("Case2: Making Background Connections");
    //Fill up to Max connections, check if we called makebackgroundconnection function
    //background connections are created by child threads, while parents wait
    try {
    	for (int i = 0;  i < MAXCON-INITCON; i++) {
		Connection conn = mypool.getConnection();
   	   	connectionArray.addElement(conn);
    	}
    } catch (SQLException se) {
        System.out.println("ConnectionPool failed, Check output console for stack trace");
      	se.printStackTrace();
        return;
    }

    Assert.assertTrue(mypool.makeBGConn == true); // should have been called for each new conncetino in loop 
    Assert.assertTrue(mypool.waitHappened == true); //should be called for each parent while new connection made
  }

  @Test
  public void testLimitReached() {
    //since the second pool is already full and we set waitIfBusy to false for that pool
    //making a new connection should return SQLException Connection limit reached
    System.out.println("Case3: Testing Connection limit ");
    
    try {
	Connection conn = mypoolDoesntWait.getConnection();
    } catch (SQLException se) { 
    	Assert.assertTrue( se.getMessage().equals("Connection limit reached"));
    }
  }

  @Test
  public void testCloseConnection() {	  
    // check numavailconn, if it's zer0, release a connection 
    // to move it from busy to avail 
    // next, set origsize = get size of pool's avail connections
    // delete 1 and assert numavailableconncetion = origsize - 1
    // Then try to delete the same connection again and assert that
    // you receive the sqlexception Cannot close this conncetion, it doesnt exist
    System.out.println("Case4: Closing Connections ");
    try {
        Connection myconn = connectionArray.get(0);
	if(mypool.numAvailableConnections() == 0) {
		mypool.releaseConnection(myconn);
	}
        int origsize = mypool.numAvailableConnections();	
        mypool.closeConnection(myconn);
	int newsize = mypool.numAvailableConnections();
	Assert.assertTrue( newsize == (origsize - 1));
	mypool.closeConnection(myconn);
    } catch (SQLException se) {
    	Assert.assertTrue(se.getMessage().equals("Cannot close this connection, it doesnt exist"));
    }
  }




} //public class Test2



