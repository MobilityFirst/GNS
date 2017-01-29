
package edu.umass.cs.gnsclient.client.util.keystorage;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;


public class DerbyControl {


  private String framework = "embedded";
  private String protocol = "jdbc:derby:";


  public Connection start() {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      GNSClientConfig.getLogger().severe("Could not find Derby driver class!");
      e.printStackTrace();
      return null;
    }
    Connection conn = null;
    GNSClientConfig.getLogger().log(Level.FINE, "Derby starting in {0} mode", framework);
    Properties props = new Properties(); // connection properties
    // providing a user name and password is optional in the embedded
    // and derbyclient frameworks
    props.put("user", "user1");
    props.put("password", "user1");


    String dbName = "clientKeyDB"; // the name of the database


    String url = protocol + dbName + ";create=true";
    //START HERE
    try {
      conn = DriverManager.getConnection(url, props);
      GNSClientConfig.getLogger().log(Level.INFO, "Connected to and created database {0}", dbName);
      // If you want to control transactions manually uncomment this line.
      //conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
      //GNSClient.getLogger().info("Problem connecting to database" + dbName + " :" + e);
      return null;
    }
  }


  public void shutdown() {

    if (framework.equals("embedded")) {
      try {
        // the shutdown=true attribute shuts down Derby
        DriverManager.getConnection("jdbc:derby:;shutdown=true");

        // To shut down a specific database only, but keep the
        // engine running (for example for connecting to other
        // databases), specify a database in the connection URL:
        //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
      } catch (SQLException se) {
        if (((se.getErrorCode() == 50000)
                && ("XJ015".equals(se.getSQLState())))) {
          // we got the expected exception
          GNSClientConfig.getLogger().info("Derby shut down normally");
          // Note that for single database shutdown, the expected
          // SQL state is "08006", and the error code is 45000.
        } else {
          // if the error code or SQLState is different, we have
          // an unexpected exception (shutdown failed)
          GNSClientConfig.getLogger().info("Derby did not shut down normally");
          printSQLException(se);
        }
      }
    }
  }


  private void reportFailure(String message) {
    GNSClientConfig.getLogger().warning("\nData verification failed:");
    GNSClientConfig.getLogger().log(Level.WARNING, "\t{0}", message);
  }


  public static void printSQLException(SQLException e) {
    // Unwraps the entire exception chain to unveil the real cause of the
    // Exception.
    while (e != null) {
      GNSClientConfig.getLogger().severe("\n----- SQLException -----");
      GNSClientConfig.getLogger().log(Level.SEVERE, "  SQL State:  {0}", e.getSQLState());
      GNSClientConfig.getLogger().log(Level.SEVERE, "  Error Code: {0}", e.getErrorCode());
      GNSClientConfig.getLogger().log(Level.SEVERE, "  Message:    {0}", e.getMessage());
      // for stack traces, refer to derby.log or uncomment this:
      //e.printStackTrace(System.err);
      e = e.getNextException();
    }
  }


  void go(String[] args) {

    parseArguments(args);


    Connection conn = null;
    ArrayList<Statement> statements = new ArrayList<>(); // list of Statements, PreparedStatements
    PreparedStatement psInsert;
    PreparedStatement psUpdate;
    Statement s;
    ResultSet rs = null;
    try {
      conn = start();


      s = conn.createStatement();
      statements.add(s);

      // We create a table...
      s.execute("create table location(num int, addr varchar(40))");
      System.out.println("Created table location");

      // and add a few rows...


      // parameter 1 is num (int), parameter 2 is addr (varchar)
      psInsert = conn.prepareStatement(
              "insert into location values (?, ?)");
      statements.add(psInsert);

      psInsert.setInt(1, 1956);
      psInsert.setString(2, "Webster St.");
      psInsert.executeUpdate();
      System.out.println("Inserted 1956 Webster");

      psInsert.setInt(1, 1910);
      psInsert.setString(2, "Union St.");
      psInsert.executeUpdate();
      System.out.println("Inserted 1910 Union");

      // Let's update some rows as well...
      // parameter 1 and 3 are num (int), parameter 2 is addr (varchar)
      psUpdate = conn.prepareStatement(
              "update location set num=?, addr=? where num=?");
      statements.add(psUpdate);

      psUpdate.setInt(1, 180);
      psUpdate.setString(2, "Grand Ave.");
      psUpdate.setInt(3, 1956);
      psUpdate.executeUpdate();
      System.out.println("Updated 1956 Webster to 180 Grand");

      psUpdate.setInt(1, 300);
      psUpdate.setString(2, "Lakeshore Ave.");
      psUpdate.setInt(3, 180);
      psUpdate.executeUpdate();
      System.out.println("Updated 180 Grand to 300 Lakeshore");



      rs = s.executeQuery(
              "SELECT num, addr FROM location ORDER BY num");


      int number; // street number retrieved from the database
      boolean failure = false;
      if (!rs.next()) {
        failure = true;
        reportFailure("No rows in ResultSet");
      }

      if ((number = rs.getInt(1)) != 300) {
        failure = true;
        reportFailure(
                "Wrong row returned, expected num=300, got " + number);
      }

      if (!rs.next()) {
        failure = true;
        reportFailure("Too few rows");
      }

      if ((number = rs.getInt(1)) != 1910) {
        failure = true;
        reportFailure(
                "Wrong row returned, expected num=1910, got " + number);
      }

      if (rs.next()) {
        failure = true;
        reportFailure("Too many rows");
      }

      if (!failure) {
        System.out.println("Verified the rows");
      }

      // delete the table
      s.execute("drop table location");
      System.out.println("Dropped table location");


      conn.commit();
      System.out.println("Committed the transaction");
      shutdown();
    } catch (SQLException sqle) {
      printSQLException(sqle);
    } finally {
      // release all open resources to avoid unnecessary memory usage

      // ResultSet
      try {
        if (rs != null) {
          rs.close();
          rs = null;
        }
      } catch (SQLException sqle) {
        printSQLException(sqle);
      }

      // Statements and PreparedStatements
      int i = 0;
      while (!statements.isEmpty()) {
        // PreparedStatement extend Statement
        Statement st = statements.remove(i);
        try {
          if (st != null) {
            st.close();
            st = null;
          }
        } catch (SQLException sqle) {
          printSQLException(sqle);
        }
      }

      //Connection
      try {
        if (conn != null) {
          conn.close();
          conn = null;
        }
      } catch (SQLException sqle) {
        printSQLException(sqle);
      }
    }
  }


  private void parseArguments(String[] args) {
    if (args.length > 0) {
      if (args[0].equalsIgnoreCase("derbyclient")) {
        framework = "derbyclient";
        protocol = "jdbc:derby://localhost:1527/";
      }
    }
  }


  public static void main(String[] args) {
    new DerbyControl().go(args);
    System.out.println("SimpleApp finished");
  }
}
