
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gigapaxos.interfaces.Summarizable;
import edu.umass.cs.gnscommon.utils.JSONDotNotation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.main.GNSConfig;

import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;

import java.util.List;
import java.util.logging.Level;
import org.json.JSONArray;


public class ValuesMap extends JSONObject implements Summarizable {


  public ValuesMap() {
    super();
  }


  // Might be redundant, but there's no obvious way to copy a JSONObject in the current lib.
  // Makes a fresh JSONObject
  public ValuesMap(JSONObject json) {
    super();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        super.put(key, json.get(key));
      } catch (JSONException e) {
        GNSConfig.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }


  public ValuesMap removeInternalFields() {
    ValuesMap copy = new ValuesMap(this);
    Iterator<?> keyIter = copy.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      if (InternalField.isInternalField(key)) {
        keyIter.remove();
      }
    }
    return copy;
  }


  //
  public JSONObject toJSONObjectFirstOnes() throws JSONException {
    JSONObject json = new JSONObject();
    addFirstOneToJSONObject(json);
    return json;
  }

  private void addFirstOneToJSONObject(JSONObject json) throws JSONException {
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      json.put(key, super.getJSONArray(key).get(0));
    }
  }


  public List<String> getKeys() throws JSONException {
    List<String> result = new ArrayList<>();
    Iterator<?> keyIter = keys();
    while (keyIter.hasNext()) {
      result.add((String) keyIter.next());
    }
    return result;
  }


  @Override
  public boolean has(String key) {
    // if key is "flapjack.sally" this returns true if the map looks like this
    // {"flapjack.sally":{"left":"eight","right":"seven"}}
    // or this
    // {"flapjack":{sally":{"left":"eight","right":"seven"}}}
    return super.has(key) || JSONDotNotation.containsFieldDotNotation(key, this);
  }


  public ResultValue getAsArray(String key) {
    try {
      // handles this case: // {"flapjack.sally":{"left":"eight","right":"seven"}}
      if (super.has(key)) {
        return new ResultValue(JSONUtils.JSONArrayToArrayList(super.getJSONArray(key)));
      }
      // handles this case: // {"flapjack":{sally":{"left":"eight","right":"seven"}}}
      if (JSONDotNotation.containsFieldDotNotation(key, this)) {
        Object object = JSONDotNotation.getWithDotNotation(key, this);
        if (object instanceof JSONArray) {
          return new ResultValue(JSONUtils.JSONArrayToArrayList((JSONArray) object));
        }
        throw new JSONException("JSONObject[" + quote(key) + "] is not a JSONArray.");

        //return new ResultValue(JSONUtils.JSONArrayToArrayList((JSONArray)getWithDotNotation(key, this)));
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON array: {0}", e);
      e.printStackTrace();
      return new ResultValue();
    }
  }


  public void putAsArray(String key, ResultValue value) {
    try {
      JSONDotNotation.putWithDotNotation(this, key, new JSONArray(value));
      //super.put(key, value);
      //GNS.getLogger().severe("@@@@@AFTER PUT (key =" + key + " value=" + value + "): " + newContent.toString());
    } catch (JSONException e) {
      e.printStackTrace();
      GNSConfig.getLogger().log(Level.SEVERE, "Unable to add JSON array to JSON Object: {0}", e);
    }
  }


  public boolean writeToValuesMap(ValuesMap destination) {
    boolean somethingChanged = false;
    Iterator<?> keyIter = keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        //destination.put(key, super.get(key));
        JSONDotNotation.putWithDotNotation(destination, key, super.get(key));
        somethingChanged = true;
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE,
                "Unable to write {0} field to ValuesMap:{1}", new Object[]{key, e});
      }
    }
    return somethingChanged;
  }


  @Override
  public Object getSummary() {
    return edu.umass.cs.utils.Util.truncate(ValuesMap.this.toString(), 64, 64).toString();
  }
}
