
package edu.umass.cs.gnsserver.gnsapp.packet;

import org.json.JSONException;
import org.json.JSONObject;


public abstract class BasicPacketWithSignatureInfo extends BasicPacketWithClientAddress {


  private final static String ACCESSOR = "si_accessor";


  private final static String SIGNATURE = "si_signature";


  private final static String MESSAGE = "si_message";
  //
  private final String accessor;
  private final String signature;
  private final String message;


  public BasicPacketWithSignatureInfo(JSONObject json) throws JSONException {
    super(json);
    this.accessor = json.optString(ACCESSOR, null);
    this.signature = json.optString(SIGNATURE, null);
    this.message = json.optString(MESSAGE, null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (accessor != null) {
      json.put(ACCESSOR, accessor);
    }
    if (signature != null) {
      json.put(SIGNATURE, signature);
    }
    if (message != null) {
      json.put(MESSAGE, message);
    }
  }


  public String getAccessor() {
    return accessor;
  }


  public String getSignature() {
    return signature;
  }


  public String getMessage() {
    return message;
  }
}
