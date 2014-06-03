package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.util.NSResponseCode;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 *
 * THIS WILL BE CHANGED WHEN WE GO TO THE NEW FULLY DEEP JSON DATA REPRESENTATION.
 * IN PARTICULAR THE RETURN VALUE WILL MOST LIKELY BECOME A JSON OBJECT INSTEAD OF A STRING.
 */
public class CommandResponse {

  /**
   * Value returned... probably will become a JSONObject soon.
   */
  private String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private NSResponseCode errorCode;
  /**
   * The RTT as measured from the LNS out and back.
   */
  private long LNSRoundTripTime; // how long this query took
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private int responder;

  public CommandResponse(String returnValue, NSResponseCode errorCode, long LNSRoundTripTime, int responder) {
    this.returnValue = returnValue;
    this.errorCode = errorCode;
    this.LNSRoundTripTime = LNSRoundTripTime;
    this.responder = responder;
  }

  public CommandResponse(String returnValue) {
    this(returnValue, NSResponseCode.NO_ERROR, -1, -1);
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

  public int getResponder() {
    return responder;
  }


  
}
