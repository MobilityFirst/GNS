package edu.umass.cs.gns.client;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ValuesMap;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * A frontend to the the database which stores the fields and values.
 * Provides conversion between the database to java objects
 *
 *
 * @author westy
 */
public class FieldAccess {

  // make it a singleton class
  public static FieldAccess getInstance() {
    return FieldAccessHolder.INSTANCE;
  }

  private static class FieldAccessHolder {

    private static final FieldAccess INSTANCE = new FieldAccess();
  }

  public String lookup(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(guid, key);
    if (result != null) {
      return new JSONArray(result).toString();
    } else {
      return new String();
    }
  }

  public String lookupMultipleValues(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ValuesMap result = client.sendMultipleReturnValueQuery(guid, key, true);
    try {
      if (result != null) {
        return result.toJSONObject().toString();
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing multiple value return:" + e);
    }
    return new String();
  }

  public String lookupOne(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(guid, key);
    if (result != null && !result.isEmpty()) {
      return (String) result.get(0);
    } else {
      return new String();
    }
  }

  public String lookupOneMultipleValues(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ValuesMap result = client.sendMultipleReturnValueQuery(guid, key, true);
    try {
      if (result != null) {
        // Pull the first value out of each array
        return result.toJSONObjectFirstOnes().toString();
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing multiple value return:" + e);
    }
    return new String();
  }

  public boolean update(String guid, String key, ResultValue value, ResultValue oldValue, UpdateOperation operation) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, key, value, oldValue, operation);
  }

  public boolean create(String guid, String key, ResultValue value) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, key, value, null, UpdateOperation.CREATE);
  }

  public String select(String key, Object value) {
    String result = SelectHandler.sendSelectRequest(new NameRecordKey(key), value);
    if (result != null) {
      return result;
    } else {
      return new String();
    }
  }
  public static String Version = "$Revision$";
}
