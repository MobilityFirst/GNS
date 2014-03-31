package edu.umass.cs.gns.database;

import edu.umass.cs.gns.main.GNS;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
/**************** FIXME All functionality of this package is provided currently by class nsdesign/recordMap/MongoRecords.java.
 * FIXME Make changes to that file until we include this package again.. **/
/**
 * Abstraction of a database lookup. 
 * @author westy
 */
public class Connect {
  
  public static boolean LocalTableOnly = true;

  private static final String local_db_ip = "127.0.0.1";
  private static final String local_db_user = "root";
  private static final String local_db_pass = "toorbar";
  // move these into properties
  private static final String remote_db_ip = "mysql.westy.org";
  private static final String remote_db_user = "mfroot";
  private static final String remote_db_pass = "toorbar";
  // the name of the database.
  private static final String local_db_name = "gnrs";
  private static final String remote_db_name = "gnrs";
  // each thread has it's own connection
  private static ThreadLocal<Connection> connection = new ThreadLocal<Connection>();

  /**
   * Returns a database connection.
   * @return 
   */
  public static Connection getConnection() throws SQLException {
    if (connection == null) {
      connection = new ThreadLocal<Connection>();
    }
    if (connection.get() != null) {
      if (!connection.get().isValid(0)) {
        GNS.getLogger().finer("Database connection needs to be reconnected");
        connection.get().close();
        connection.set(null);
      }
    }
    if (connection.get() == null) {
      try {
        String url = "jdbc:mysql://" + local_db_ip + "/" + local_db_name;
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        connection.set(DriverManager.getConnection(url, local_db_user, local_db_pass));
        GNS.getLogger().finer("Local database connection established");
      } catch (ClassNotFoundException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      } catch (IllegalAccessException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      } catch (InstantiationException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      }
//      } catch (Exception e) {
//        GNRS.getLogger().severe("Error... Cannot connect to database server: " + e);
//      }
    }
    if (connection.get() == null && LocalTableOnly == false) {
      try {
        String url = "jdbc:mysql://" + remote_db_ip + "/" + remote_db_name;
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        connection.set(DriverManager.getConnection(url, remote_db_user, remote_db_pass));
        GNS.getLogger().finer("Remote database connection established");
      } catch (ClassNotFoundException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      } catch (IllegalAccessException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      } catch (InstantiationException e) {
        throw new SQLException("Error: " + e.getMessage(), e);
      }
    }
    return connection.get();
  }

  /**
   * Closes a database connection.
   */
  public static void closeConnection() {
    if (connection != null) {
      try {
        connection.get().close();
        connection = null;
        GNS.getLogger().finer("Database connection terminated");
      } catch (Exception e) { /* ignore close errors */ }
    }
  }

  public static void main(String[] args) throws SQLException{

    Connection conn = getConnection();
    closeConnection();
  }
}
