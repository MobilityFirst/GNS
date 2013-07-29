/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.database;

import edu.umass.cs.gnrs.main.GNS;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class MYSQLRecordTableEntry {

  private String guid;
  private JSONObject jsonObject;

  public String getGuid() {
    return guid;
  }

  public JSONObject getJsonObject() {
    return jsonObject;
  }

  static MYSQLRecordTableEntry createFromResultSet(ResultSet rs) throws JSONException, SQLException{
    MYSQLRecordTableEntry result = new MYSQLRecordTableEntry();
    //GNRS.getLogger().info("**createFromResultSet**: id = " + rs.getInt("id") + ", guid = " + rs.getString(RecordTable.GUID) + ", json = " + rs.getString(RecordTable.JSON));
    int idVal = rs.getInt("id");
    result.guid = rs.getString(MYSQLRecordTable.GUID);
    result.jsonObject = new JSONObject(rs.getString(MYSQLRecordTable.JSON));
    GNS.getLogger().finer("id = " + idVal + ", guid = " + result.guid + ", json = " + result.jsonObject);
    return result;
  }

  @Override
  public String toString() {
    return "RecordTableEntry{" + "guid=" + guid + ", jsonObject=" + jsonObject + '}';
  }
}
