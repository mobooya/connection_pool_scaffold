package com.opower.connectionpool;
import java.util.*;
import java.sql.*;

public class myConnectionPool implements ConnectionPool, Runnable {
	private String driver, url, username, password;
	private int maxConnections;
	private boolean waitIfBusy;

	private Vector availableConnections, busyConnections;
	private boolean connectionPending = false;
	
	public boolean makeBGConn = false; //for testing: makeBackgroundConnection has been called
	public boolean waitHappened = false; //for testing: a thread is waiting

	//constructor
   	public myConnectionPool (String driver, String url, String username,
			String password, int initialConnections, int maxConnections,
			boolean waitIfBusy) throws SQLException {
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		this.maxConnections = maxConnections;
		this.waitIfBusy = waitIfBusy;
		if (initialConnections > maxConnections) {
			initialConnections = maxConnections;
		}
		availableConnections = new Vector(initialConnections);
		busyConnections = new Vector();
		for (int i = 0; i < initialConnections; i++) {
			availableConnections.addElement(makeNewConnection());
		}
	}

	//use Java datbase conncetion api (jdbc)
	private Connection makeNewConnection() throws SQLException {
		try {
			//Load JDBC driver class if not already loaded
			//this is specific to the type of database e.g. mysql
			//Class.forName loads the driver 
			Class.forName(driver).newInstance();
			//obtain a connection using the JDBC DriverManager class
			Connection connection = DriverManager.getConnection(url, username, password);
			return connection;
		} catch (InstantiationException ie) {
			System.out.println("InstantiationException");
			ie.printStackTrace();
		} catch (IllegalAccessException iae) {
			System.out.println("IllegalAccessException");
			iae.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException Message: " + e.getMessage());
			//e.printStackTrace();
			throw new SQLException("Can't find class for this driver you provided: " + driver);
		}
		return null;
	}


        /**
     	* Releases a connection back into the connection pool.
    	* 
    	* @param connection the connection to return to the pool
    	* @throws java.sql.SQLException
    	*/
	//The user of this connectionpool must free up that connection when he is 
	//done using it or when it times out, so that others can use it

    	public synchronized void releaseConnection(Connection connection) throws SQLException {
		if (!busyConnections.contains(connection)) {
			throw new SQLException("Cant release this connection because its not being used.");
			//not a fatal error, Processing will continue
		} else {
			busyConnections.removeElement(connection);
		    	availableConnections.addElement(connection);
		}
		// Wake up threads that are waiting for a connection
	    	notifyAll(); 
	}
    
  	public synchronized int totalConnections() {
    		return(availableConnections.size() + busyConnections.size());
  	}


    	/**
     	* Gets a connection from the connection pool.
     	* 
     	* @return a valid connection from the pool.
     	*/
    	public synchronized Connection getConnection() throws SQLException {
	    //The easy case is when there are available connections in the pool
	    //so pick one (move it from avail vector to busy vector)
	    if (!availableConnections.isEmpty()) {
		    //Typecast the vector element to a Connection type
		    Connection existingConnection = (Connection) availableConnections.lastElement();
		    int lastIndex = availableConnections.size() - 1;
		    availableConnections.removeElementAt(lastIndex);
		    //Before we use this availableconnection, check if it timed out
		    //if it did, then dont use this one, recurse this function to pick another
		    //availableconncetion or make a new one if we havent reach maxconnections
		    //if its not timed out, put it in busyconnections and return it
		    if (existingConnection.isClosed()) {
			    //since this function is a critical section of code
			    //only 1 thread can try to get a conncetion at a time
			    //since this current thread could not, we now give the 
			    //others a chance to try getting a connection
			    notifyAll();
			    //but this current thread also needs to try again
			    //so recursively call this function
			    return (getConnection());
		    }
		    else {
			    busyConnections.addElement(existingConnection);
			    return existingConnection;
		    }
	    } else {
		    //If available + busy is less than max, then we can create another
		    //new connection to use.  but if we maxed out, then its up to the user
		    //to decide what he wants to do.  the user gives us this in a parameter
		    //called waitIfBusy.  this is their preference to either wait until a 
		    //connection is available, or just fail and let them know that all
		    //connections were busy.  this is useful if the user has multiple 
		    //connection pools to try getting connections from.

		    //if we are not maxed out, we will create a new connection, but
		    //we will only do this 1 at a time to prevent multiple threads creating
		    //more connections than we have room for in the maxconnections.
		    if ((totalConnections() < maxConnections) && !connectionPending) {
			    //This will create a new thread to run in the background
			    //That thread will be running in parallel to this one
			    //because creating a connection is not a synchronized
			    //function, so it doesnt need the lock on ConnPoolclass
			    //but eventually that thread will reach (in the run method)
			    //a synchronized block on this (the ConnPoolClass), and
			    //since the lock is already taken by getConnection, it will
			    //wait until getConnection gives up the lock
			    //In parallel, get connection returned from makeBackGround
			    //and continues until it sees wait().  that forces it to give
			    //up its lock on the ConPool class, so when makeBackgorun reaches
			    //it or if it already reached it, makeBackground will obtain that
			    //lock and proceed to update the available Connections 
			    makeBackgroundConnection();
		    } else if (!waitIfBusy) {
			    throw new SQLException("Connection limit reached");
		    }

		    //wait for a new conncetion to be made in the background or
		    //for an existing connection to be released back into the available
		    try {
			    //as we mentioned, wait will make this thread go to sleep until
			    //it is woken up by notify
			    waitHappened = true; //for testing
			    wait();
			    //we are waiting until the background thread does notifyall and
			    //finishes that code in its synchronized block to give up the lock
			    //because even if you are awake, you cannot do anything until you 
			    //obtain the lock
		    } catch (InterruptedException ie) {} //an interrupt is still checked exception
		
	            //we are done waiting so we know there is a new conection available, lets 
		    //try to get it by calling this function again recursively
		    return (getConnection());
	    }
    	}

    	private void makeBackgroundConnection() {
	   makeBGConn = true; //for testing only
	   connectionPending = true; 
	   //if a another thread besides the original one calls getConnection
	   //then they will see a connectio is pending and skip down to the wait instead of calling
	   //makeground again
	   try {
		Thread connectThread = new Thread(this); //conpool class implements runnable interface
		connectThread.start();
	   } catch (OutOfMemoryError oome) {
		//give up on making a new connection, wait until existing one frees up
	   }
        }


        public void run() {
	    try {
		    Connection connection = makeNewConnection();
		    synchronized(this) {
			    availableConnections.addElement(connection);
			    connectionPending = false;
			    notifyAll();
		    }
	    } catch (Exception e) { //sqlexception or outofmemoryerror 
		//give up on making a new connection, wait until existing one frees up
	    }
        }

 	public synchronized String toString() {
    		String info =
      			"ConnectionPool(" + url + "," + username + ")" +
      			", available=" + availableConnections.size() +
      			", busy=" + busyConnections.size() +
      			", max=" + maxConnections;
    		return(info);
  	}
	public synchronized void closeAllConnections() {
	    closeConnections(availableConnections);
	    availableConnections = new Vector();
	    closeConnections(busyConnections);
	    busyConnections = new Vector();
	}

	private void closeConnections(Vector connections) {
	    try {
	      for(int i=0; i<connections.size(); i++) {
	        Connection connection =
	          (Connection)connections.elementAt(i);
	        if (!connection.isClosed()) {
	          connection.close();
	        }
	      }
	    } catch(SQLException se) {
	      // Ignore errors; garbage collect anyhow
	    }
	}

	//Close 1 connection (remove it from busy or available)
	public void closeConnection(Connection conn) throws SQLException {

		boolean inbusy = busyConnections.contains(conn);
		boolean inavail = availableConnections.contains(conn);
		if (!inbusy && !inavail){
			throw new SQLException("Cannot close this connection, it doesnt exist");
		} else if (inbusy) {
			busyConnections.removeElement(conn);
		} else if (inavail) {
			availableConnections.removeElement(conn);
		}
	}

}
