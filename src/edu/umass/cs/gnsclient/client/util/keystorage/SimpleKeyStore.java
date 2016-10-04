/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util.keystorage;

import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import static edu.umass.cs.gnscommon.utils.Format.formatPrettyDateUTC;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Provides get, put, remove, etc for String keysStatement and values using
 * an underlying embedded Derby database.
 *
 * We use a trigger to keep track of update times for each row.
 * We explicitly set a timestamp field for each read on the row.
 *
 * @author westy, ayadav
 */
public class SimpleKeyStore extends AbstractKeyStorage
{

  public static final int MAX_KEY_LENGTH = 2048;

  private static final String TABLE_NAME = "GNS_CLIENT_KEYSTORE"; // Derby uses all upcase for table names
  private static final String TABLE_CREATE = "(KEYFIELD varchar(2048) not null, "
          + "VALUEFIELD varchar(15360), "
          + "READTIME TIMESTAMP, "
          + "UPDATETIME TIMESTAMP, "
          //+ "DEFAULT CURRENT_TIMESTAMP " // not in derby
          //+ "ON UPDATE CURRENT_TIMESTAMP, " // not in derby
          + "primary key (KEYFIELD))";

  private static final String UPDATE_TRIGGER = "CREATE TRIGGER updateTimestamp "
          + "AFTER UPDATE OF VALUEFIELD ON " + TABLE_NAME + " "
          + "REFERENCING OLD AS EXISTING "
          + "FOR EACH ROW MODE DB2SQL "
          + "UPDATE " + TABLE_NAME + " SET UPDATETIME = CURRENT_TIMESTAMP "
          + "WHERE KEYFIELD = EXISTING.KEYFIELD";

  private DerbyControl derbyControl = new DerbyControl();
  private Connection conn = null;

  /**
   * Creates a new instance of a key store.
   */
  public SimpleKeyStore() {
    GNSClientConfig.getLogger().fine("Attempting to connect and create table " + TABLE_NAME);

    conn = derbyControl.start();
    if (conn == null) {
      GNSClientConfig.getLogger().severe("Problem starting derby!");
      return;
    }
    // We create a table...
    maybeCreateTable(TABLE_NAME, TABLE_CREATE, UPDATE_TRIGGER);

  }

  /**
   * Frees up all the resources used by the key store.
   */
  public void shutdown() {
    derbyControl.shutdown();
  }

  /**
   * Associates the specified value with the specified key.
   *
   * @param key
   * @param value
   */
  public void put(String key, String value) {
    try {
      if (get(key, null) != null) {
        PreparedStatement updateStatement = conn.prepareStatement("UPDATE " + TABLE_NAME
                + " SET VALUEFIELD=? WHERE KEYFIELD=?");
        updateStatement.setString(1, value);
        updateStatement.setString(2, key);
        updateStatement.executeUpdate();
        //conn.commit();
      } else {
        PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO " + TABLE_NAME
                + " VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");
        insertStatement.setString(1, key);
        insertStatement.setString(2, value);
        insertStatement.executeUpdate();
        //conn.commit();
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  /**
   * Returns the value associated with the specified key.
   * Returns the specified default if there is no value associated
   * with the key or if some problem arises.
   *
   * @param key
   * @param def
   * @return
   */
  public String get(String key, String def) {
    String result = def;
    ResultSet rs = null;
    try {
      PreparedStatement getStatement
              = conn.prepareStatement("select VALUEFIELD from " + TABLE_NAME + " where KEYFIELD=?");
      getStatement.setString(1, key);
      rs = getStatement.executeQuery();
      if (rs.next()) {
        result = rs.getString("VALUEFIELD");
        // We explicitly set a timestamp field for each read on the row.
        updateReadTime(key);
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    } finally {
      safelyClose(rs);
    }
    return result;
  }

  private boolean suppressUpdateRead = false;

  private void updateReadTime(String key) throws SQLException {
    if (!suppressUpdateRead) {
      PreparedStatement updateStatement = conn.prepareStatement("UPDATE " + TABLE_NAME
              + " SET READTIME=? WHERE KEYFIELD=?");
      updateStatement.setTimestamp(1, new Timestamp(new Date().getTime()));
      updateStatement.setString(2, key);
      updateStatement.executeUpdate();
    }
  }

  public Date updateTime(String key) {
    ResultSet rs = null;
    try {
      PreparedStatement getStatement
              = conn.prepareStatement("select UPDATETIME from " + TABLE_NAME + " where KEYFIELD=?");
      getStatement.setString(1, key);
      rs = getStatement.executeQuery();
      if (rs.next()) {
        return rs.getTimestamp("UPDATETIME");
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    } finally {
      safelyClose(rs);
    }
    return null;
  }

  public Date readTime(String key) {
    ResultSet rs = null;
    try {
      PreparedStatement getStatement 
              = conn.prepareStatement("select READTIME from " + TABLE_NAME + " where KEYFIELD=?");
      getStatement.setString(1, key);
      rs = getStatement.executeQuery();
      if (rs.next()) {
        return rs.getTimestamp("READTIME");
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    } finally {
      safelyClose(rs);
    }
    return null;
  }

  /**
   * Removes the value associated with the specified key.
   *
   * @param key
   */
  public void remove(String key) {
    try {
      PreparedStatement removeStatement 
              = conn.prepareStatement("delete from " + TABLE_NAME + " where KEYFIELD=?");
      removeStatement.setString(1, key);
      removeStatement.executeUpdate();
      //conn.commit();
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  /**
   * Removes all of the key-value associations .
   */
  public void clear() {
    try {
      PreparedStatement clearStatement = conn.prepareStatement("truncate table " + TABLE_NAME);
      clearStatement.executeUpdate();
      //conn.commit();
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  /**
   * Returns all of the keys that have an associated value.
   * (The returned array will be of size zero if there are none.)
   *
   * @return all the keys
   */
  public String[] keys() {
    List<String> keys = new ArrayList<>();
    try {
      PreparedStatement keysStatement = conn.prepareStatement("select KEYFIELD from " + TABLE_NAME);
      ResultSet rs = keysStatement.executeQuery();
      while (rs.next()) {
        keys.add(rs.getString(1));
      }
      String[] keyArray = new String[keys.size()];
      return keys.toArray(keyArray);
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
    return ArrayUtils.EMPTY_STRING_ARRAY;
  }

  private void safelyClose(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  private void safelyClose(PreparedStatement p) {
    try {
      if (p != null) {
        p.close();
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  private boolean tableExists(String name) {
    try {
      DatabaseMetaData dbm = conn.getMetaData();
      ResultSet resultSet = dbm.getTables(null, null, name, null);
      return resultSet.next();
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
      return false;
    }
  }

  private void maybeCreateTable(String tableName, String creationString, String triggerStatement) {
    try {
      Statement s = conn.createStatement();
      if (!tableExists(tableName)) {
        String statement = "CREATE TABLE " + tableName + " " + creationString;
        GNSClientConfig.getLogger().log(Level.FINE, "Created table {0}", tableName);
        s.executeUpdate(statement);
        // Add the trigger
        Statement t = conn.createStatement();
        t.executeUpdate(triggerStatement);
      } else {
        GNSClientConfig.getLogger().log(Level.FINE, "Found table {0}", tableName);
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  private void dropTable(String tableName) {
    try {
      Statement s = conn.createStatement();
      if (tableExists(tableName)) {
        String statement = "DROP TABLE " + tableName;
        GNSClientConfig.getLogger().log(Level.FINE, "Statement:{0}", statement);
        s.executeUpdate(statement);
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  private void showAllTables() {
    try {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet resultSet = meta.getColumns(null, null, null, null);
      while (resultSet.next()) {
        if (!resultSet.getString("TABLE_NAME").startsWith("SYS")) {
          GNSClientConfig.getLogger().log(Level.FINE, "TABLE: {0}", resultSet.getString("TABLE_NAME"));
        }
      }
    } catch (SQLException e) {
      DerbyControl.printSQLException(e);
    }
  }

  // TEST CODE
  public static void main(String[] args) {

    SimpleKeyStore keyStore = new SimpleKeyStore();
    if (args.length == 1 && args[0].startsWith("-drop")) {
      keyStore.dropTable(TABLE_NAME);
      GNSClientConfig.getLogger().info("Dropped table " + TABLE_NAME);
      keyStore.showAllTables();
    } else {
      try {
        keyStore.suppressUpdateRead = true;
        for (String key : keyStore.keys()) {
          GNSClientConfig.getLogger().log(Level.INFO,
                  "{0} -> {1} '{'U:{2}, R:{3}'}'",
                  new Object[]{key, keyStore.get(key, null),
                    formatPrettyDateUTC(keyStore.updateTime(key)),
                    formatPrettyDateUTC(keyStore.readTime(key))});
        }
      } finally {
        keyStore.suppressUpdateRead = false;
      }
      String value = "value-" + RandomString.randomString(6);
      GNSClientConfig.getLogger().log(Level.INFO, "Putting {0}", value);
      keyStore.put("frank", value);
      GNSClientConfig.getLogger().log(Level.INFO, "New value is {0}", keyStore.get("frank", null));
      keyStore.shutdown();
    }
  }
  
  @Override
  public String toString() 
  {
	  return this.toString();
  }
}