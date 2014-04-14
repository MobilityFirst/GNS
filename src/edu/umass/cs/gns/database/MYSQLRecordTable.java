package edu.umass.cs.gns.database;

import edu.umass.cs.gns.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
/**
 * A front end to the mySQL table which stores the fields and values
 *
 * @author westy
 */
public class MYSQLRecordTable {

  private static final String TableNameRoot = "maintable";
  public static final String GUID = "guid";
  public static final String JSON = "jsonobject";
  private static final String TableCreate = "(id INT UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY (id), " + GUID + " CHAR(40), " + JSON + " TEXT)";
  private String tableName;

  public MYSQLRecordTable(int nodeID) {
    tableName = TableNameRoot + nodeID;
    MySQLUtils.dropTable(tableName);
    MySQLUtils.maybeCreateTable(tableName, TableCreate);
  }

  // for debugging
  public String getTableName() {
    return tableName;
  }

  // make it a singleton class
//  public static MYSQLRecordTable getInstance() {
//
//    return RecordTableHolder.INSTANCE;
//  }

//  private static class RecordTableHolder {
//
//    private static final MYSQLRecordTable INSTANCE = new MYSQLRecordTable();
//  }

  private String tableStandardQuery() {
    return "SELECT id, " + GUID + ", " + JSON + " FROM " + tableName;
  }

  private String RecordTableUpdate(String guid, JSONObject jsonObject) {
    return "SET " + GUID + " ='" + guid + "', " + JSON + " ='" + jsonObject.toString() + "'";
  }

  private MYSQLRecordTableEntry lookupHelper(String guid) {
    MYSQLRecordTableEntry result = null;
    try {
      Connection conn = Connect.getConnection();
      Statement s = conn.createStatement();
      String statement = tableStandardQuery() + " WHERE " + GUID + " = '" + guid + "'";
      GNS.getLogger().finer("Statement:" + statement);
      s.executeQuery(statement);

      ResultSet rs = s.getResultSet();
      if (rs.next()) {
        result = MYSQLRecordTableEntry.createFromResultSet(rs);
      }
      rs.close();
      s.close();
    } catch (SQLException e) {
      GNS.getLogger().severe("Error... problem executing statement : " + e);
      e.printStackTrace();
    } catch (JSONException e) {
      GNS.getLogger().severe("Error... problem getting JSON value : " + e);
      e.printStackTrace();
    }
    return result;
  }

  public String lookup(String guid, String key) {
    MYSQLRecordTableEntry entry = lookupHelper(guid);
    if (entry != null) {
      String value = entry.getJsonObject().optString(key, null);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public boolean contains(String guid, String key) {
    MYSQLRecordTableEntry entry = lookupHelper(guid);
    if (entry == null) {
      return false;
    } else if (entry.getJsonObject().optString(key, "").equals("")) {
      return false;
    } else {
      return true;
    }
  }

  private JSONObject lookup(String guid) {
    JSONObject jsonObject;
    MYSQLRecordTableEntry entry = lookupHelper(guid);
    if (entry == null) {
      return new JSONObject();
    } else {
      return entry.getJsonObject();
    }
  }

  public void update(String guid, String key, JSONObject value) {
    try {
      JSONObject jsonObject = lookup(guid);
      jsonObject.put(key, value);
      writeJSONObject(guid, jsonObject);
    } catch (JSONException e) {
      GNS.getLogger().severe("Error... problem updating JSON value : " + e);
      e.printStackTrace();
    }
  }

  public void remove(String guid, String key) {
    JSONObject jsonObject = lookup(guid);
    jsonObject.remove(key);
    writeJSONObject(guid, jsonObject);
  }

  private void writeJSONObject(String guid, JSONObject jsonObject) {
    try {
      Connection conn = Connect.getConnection();
      Statement s = conn.createStatement();
      String setPart = RecordTableUpdate(guid, jsonObject);
      String updateText;
      if (lookupHelper(guid) == null) { // redundant db access... live with it
        updateText = "INSERT INTO " + tableName + " " + setPart;
      } else {
        updateText = "UPDATE " + tableName + " " + setPart + " WHERE " + GUID + " = '" + guid + "'";
      }
      GNS.getLogger().finer("Update text:" + updateText);

      s.executeUpdate(updateText);
    } catch (SQLException e) {
      GNS.getLogger().severe("Error... problem executing statement : " + e);
      e.printStackTrace();
    }
  }

  private void delete(String guid) {
    try {
      Connection conn = Connect.getConnection();
      Statement s = conn.createStatement();
      String statement = "DELETE FROM " + tableName + " WHERE " + GUID + " = '" + guid + "'";
      GNS.getLogger().finer("Statement:" + statement);
      s.executeUpdate(statement);
    } catch (SQLException e) {
      GNS.getLogger().severe("Error... problem executing statement : " + e);
      e.printStackTrace();
    }
  }

  public String dump() {
    StringBuilder result = new StringBuilder();
    try {
      Connection conn = Connect.getConnection();
      Statement s = conn.createStatement();
      String statement = tableStandardQuery();
      GNS.getLogger().finer("Statement:" + statement);
      s.executeQuery(statement);
      ResultSet rs = s.getResultSet();
      while (rs.next()) {
        MYSQLRecordTableEntry entry = MYSQLRecordTableEntry.createFromResultSet(rs);
        JSONObject jsonObject = entry.getJsonObject();
        jsonObject.put("guid", entry.getGuid());
        result.append(jsonObject.toString());
        result.append("\n");
      }
      rs.close();
      s.close();
    } catch (SQLException e) {
      GNS.getLogger().severe("Error... problem executing MYSQL statement : " + e);
      e.printStackTrace();
    } catch (JSONException e) {
      GNS.getLogger().severe("Error... problem getting JSON value : " + e);
      e.printStackTrace();
    }
    if (result.length() > 0) { // trim newline
      result.setLength(result.length() - 1);
    }
    return result.toString().trim();
  }

  public ArrayList<MYSQLRecordTableEntry> retrieveAllEntries() {
    ArrayList<MYSQLRecordTableEntry> result = new ArrayList<MYSQLRecordTableEntry>();
    try {
      Connection conn = Connect.getConnection();
      Statement s = conn.createStatement();
      ResultSet rs;
      int count;

      s.executeQuery(tableStandardQuery());
      rs = s.getResultSet();
      count = 0;
      while (rs.next()) {
        ++count;
        MYSQLRecordTableEntry entry = MYSQLRecordTableEntry.createFromResultSet(rs);
        GNS.getLogger().finer(entry.toString());
        result.add(entry);
      }
      GNS.getLogger().finer(count + " entries were retrieved");
      rs.close();
      s.close();

    } catch (SQLException e) {
      GNS.getLogger().severe("Error... problem executing statement : " + e);
      e.printStackTrace();
    } catch (JSONException e) {
      GNS.getLogger().severe("Error... problem reading JSON : " + e);
      e.printStackTrace();
    } finally {
      //Connect.closeConnection();
    }
    return result;
  }

  public void printAllEntries() {
    for (MYSQLRecordTableEntry entry : retrieveAllEntries()) {
      System.out.println(entry.toString());
    }
  }

  public void resetTable() {
    MySQLUtils.dropTable(tableName);
    MySQLUtils.maybeCreateTable(tableName, TableCreate);
  }
  public static String Version = "$Revision$";
}
