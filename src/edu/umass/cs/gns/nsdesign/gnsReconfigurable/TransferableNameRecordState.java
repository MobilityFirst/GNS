package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents the state for a name record that GNS will transfer to (or received from) coordinator, and other replicas.
 *
 * Created by abhigyan on 3/29/14.
 */
public class TransferableNameRecordState {

  public final ValuesMap valuesMap;
  public final int ttl;

  public TransferableNameRecordState(ValuesMap valuesMap, int ttl) {
    this.valuesMap = valuesMap;
    this.ttl = ttl;
  }

  public TransferableNameRecordState(String state) throws JSONException{
    int ttlIndex = state.indexOf(":");
    this.ttl = Integer.parseInt(state.substring(0, ttlIndex));
    this.valuesMap = new ValuesMap(new JSONObject(state.substring(ttlIndex + 1)));
  }

  public String toString() {
    try {
      return ttl + ":" + valuesMap.toJSONObject(); // need to convert to json as it will be reinserted into database.
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in  creating state string:" + valuesMap);
      e.printStackTrace();
    }
    return null;
  }

}
