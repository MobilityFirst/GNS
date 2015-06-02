package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.util.NSResponseCode;

/**
 * Encapsulates the response values and instrumentation that we pass back from the 
 * Local Name Server to the client.
 * @param <NodeIDType>
 */
public class CommandResponse<NodeIDType> {

  /**
   * Value returned... probably will become a JSONObject soon.
   */
  private String returnValue;
  /**
   * Indicates if the response is an error. Can be null.
   */
  private NSResponseCode errorCode;
  
  // instrumentation
  /**
   * The RTT as measured from the LNS out and back.
   */
  private long LNSRoundTripTime; // how long this query took
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private NodeIDType responder;
  /**
   * Database lookup time instrumentation
   */
  private final int lookupTime;

  /**
   * Create a command response object from a return value with an error code.
   * 
   * @param returnValue
   * @param errorCode
   * @param LNSRoundTripTime
   * @param responder
   */
  public CommandResponse(String returnValue, NSResponseCode errorCode, 
          long LNSRoundTripTime, NodeIDType responder, int lookupTime) {
    this.returnValue = returnValue;
    this.errorCode = errorCode;
    this.LNSRoundTripTime = LNSRoundTripTime;
    this.responder = responder;
    this.lookupTime = lookupTime;
  }

  /**
   * Create a command response object from a return value with no error.
   * 
   * @param returnValue
   */
  public CommandResponse(String returnValue) {
    this(returnValue, NSResponseCode.NO_ERROR, -1, null, -1);
  }

  /**
   * Gets the return value.
   * 
   * @return
   */
  public String getReturnValue() {
    return returnValue;
  }

  /**
   * Gets the error code.
   * 
   * @return
   */
  public NSResponseCode getErrorCode() {
    return errorCode;
  }
  
  /**
   * Does this Command contain an error result.
   * 
   * @return 
   */
  public boolean isError() {
    return this.errorCode != null;
  }

  /**
   * Gets the LNS round trip time (computed from LNS to NS and back).
   * @return
   */
  public long getCCPRoundTripTime() {
    return LNSRoundTripTime;
  }

  /**
   * Gets the is id fd the name server that responded (could be -1 if not known).
   * 
   * @return
   */
  public NodeIDType getResponder() {
    return responder;
  }

  /**
   * Retrieves the database lookup time instrumentation from the command response.
   * 
   * @return 
   */
  public int getLookupTime() {
    return lookupTime;
  }

}
