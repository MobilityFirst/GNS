package edu.umass.cs.gns.newApp.packet;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.newApp.packet.Packet.PacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 * Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null)
 * plus instrumentation.
 *
 * THIS EXACT CLASS IS ALSO IN THE CLIENT so they need to be kept consistent
 * insofar as the fields in the JOSN Object is concerned.
 *
 */
public class CommandValueReturnPacket extends BasicPacket implements InterfaceRequest {

  private final static String CLIENTREQUESTID = "reqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SERVICENAME = "srvceName";
  private final static String RETURNVALUE = "returnValue";
  private final static String ERRORCODE = "errorCode";
  private final static String LNSROUNDTRIPTIME = "lnsRtt";
  private final static String LNSPROCESSINGTIME = "lnsTime";
  private final static String RESPONDER = "responder";
  private final static String REQUESTCNT = "requestCnt";
  private final static String REQUESTRATE = "requestRate";
  private final static String LOOKUPTIME = "lookuptime";

  /**
   * Identifier of the request.
   */
  private final int clientRequestId;
  /**
   * The service name from the request. Usually the guid or HRN.
   */
  private final String serviceName;
  /**
   * LNS identifier used by the LNS.
   */
  private final int LNSRequestId;
  /**
   * The returned value.
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final NSResponseCode errorCode;
  /**
   * The RTT as measured from the LNS out and back.
   */
  private final long LNSRoundTripTime; // how long this query took from the LNS out and back
  /**
   * Total command processing time at the LNS.
   */
  private final long LNSProcessingTime; // how long this query took inside the LNS
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private final String responder;
  /**
   * Instrumentation - the request counter from the LNS)
   */
  private final long requestCnt;
  /**
   * Instrumentation - the current requests per second from the LNS (can be used to tell how busy LNS is)
   */
  private final int requestRate;
  /**
   * Database lookup time instrumentation
   */
  private final int lookupTime;

  /**
   * Creates a CommandValueReturnPacket from a CommandResponse.
   *
   * @param requestId
   * @param response
   * @param requestCnt - current number of requests handled by the LNS (can be used to tell how busy LNS is)
   * @param requestRate
   * @param lnsProcTime
   */
  public CommandValueReturnPacket(int requestId, int LNSRequestId, String serviceName,
          CommandResponse<String> response, long requestCnt,
          int requestRate, long lnsProcTime) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.LNSRequestId = LNSRequestId;
    this.serviceName = serviceName;
    this.returnValue = response.getReturnValue();
    this.errorCode = response.getErrorCode();
    this.LNSRoundTripTime = response.getLNSRoundTripTime();
    this.LNSProcessingTime = lnsProcTime;
    this.responder = response.getResponder();
    this.requestCnt = requestCnt;
    this.requestRate = requestRate;
    this.lookupTime = response.getLookupTime();
  }

  /**
   * Creates a CommandValueReturnPacket from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandValueReturnPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.clientRequestId = json.getInt(CLIENTREQUESTID);
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getInt(LNSREQUESTID);
    } else {
      this.LNSRequestId = -1;
    }
    this.serviceName = json.getString(SERVICENAME);
    this.returnValue = json.getString(RETURNVALUE);
    if (json.has(ERRORCODE)) {
      this.errorCode = NSResponseCode.getResponseCode(json.getInt(ERRORCODE));
    } else {
      this.errorCode = NSResponseCode.NO_ERROR;
    }
    // instrumentation
    this.requestRate = json.getInt(REQUESTRATE);
    this.requestCnt = json.getLong(REQUESTCNT);
    //
    this.LNSRoundTripTime = json.optLong(LNSROUNDTRIPTIME, -1);
    this.LNSProcessingTime = json.optLong(LNSPROCESSINGTIME, -1);
    this.responder = json.has(RESPONDER) ? json.getString(RESPONDER) : null;
    this.lookupTime = json.optInt(LOOKUPTIME, -1);
  }

  /**
   * Converts the command object into a JSONObject.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(CLIENTREQUESTID, this.clientRequestId);
    if (this.LNSRequestId != -1) {
      json.put(LNSREQUESTID, this.LNSRequestId);
    }
    json.put(SERVICENAME, this.serviceName);
    json.put(RETURNVALUE, returnValue);
    if (errorCode != null) {
      json.put(ERRORCODE, errorCode.getCodeValue());
    }
    json.put(REQUESTRATE, requestRate); // instrumentation
    json.put(REQUESTCNT, requestCnt); // instrumentation
    // instrumentation
    if (LNSRoundTripTime != -1) {
      json.put(LNSROUNDTRIPTIME, LNSRoundTripTime);
    }
    if (LNSProcessingTime != -1) {
      json.put(LNSPROCESSINGTIME, LNSProcessingTime);
    }
    // instrumentation
    if (responder != null) {
      json.put(RESPONDER, responder);
    }
    if (lookupTime != -1) {
      json.put(LOOKUPTIME, lookupTime);
    }
    return json;
  }

  public int getClientRequestId() {
    return clientRequestId;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  public int getLNSRequestId() {
    return LNSRequestId;
  }

  public String getReturnValue() {
    return returnValue;
  }

  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  public long getLNSRoundTripTime() {
    return LNSRoundTripTime;
  }

  public long getLNSProcessingTime() {
    return LNSProcessingTime;
  }

  public String getResponder() {
    return responder;
  }

  public long getRequestCnt() {
    return requestCnt;
  }

  public int getRequestRate() {
    return requestRate;
  }

  public int getLookupTime() {
    return lookupTime;
  }

}
