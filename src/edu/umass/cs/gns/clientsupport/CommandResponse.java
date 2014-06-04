package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.util.NSResponseCode;

/**
 * Encapsulates the response values and instrumentation that we pass back from the 
 * Local Name Server to the client.
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
