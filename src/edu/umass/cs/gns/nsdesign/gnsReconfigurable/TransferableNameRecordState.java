package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents the state for a name record that GNS will transfer to (or received from) coordinator, and other replicas.
 *
 * Created by abhigyan on 3/29/14.
 */
public class TransferableNameRecordState {
  
  final static String SEPARATOR = ":::";

  public final ValuesMap valuesMap;
  public final int ttl;

  public TransferableNameRecordState(ValuesMap valuesMap, int ttl) {
    this.valuesMap = valuesMap;
    this.ttl = ttl;
  }

  public TransferableNameRecordState(String state) throws JSONException {
    int ttlIndex;
    if (state != null &&  (ttlIndex = state.indexOf(SEPARATOR)) != -1) {
      this.ttl = Integer.parseInt(state.substring(0, ttlIndex));
      this.valuesMap = new ValuesMap(new JSONObject(state.substring(ttlIndex + SEPARATOR.length())));
    } else if (state != null) {
      this.ttl = 0;
      this.valuesMap = new ValuesMap(new JSONObject(state));
    } else {
      this.ttl = 0;
      this.valuesMap = new ValuesMap();
    } 
  }

  @Override
  public String toString() {
    return ttl + ":::" + valuesMap; // need to convert to json as it will be reinserted into database.
  }

}
