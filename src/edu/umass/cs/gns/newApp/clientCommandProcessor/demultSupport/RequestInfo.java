package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONObject;

/**
 * Class represents the abstract class in which CCP stores info for each ongoing request,
 * from the time it is received by a CCP until a success/failure response is returned.
 * Only a single instance of this class is defined during the lifetime of a request.
 *
 * Some fields in this class are necessary to implement functionality of a local name server, while
 * some are there only to collect and log statistics for a request.
 *
 * Created by abhigyan on 5/29/14.
 */
public abstract class RequestInfo {

  protected String name;

  /** Unique request ID assigned to this request by the local name server */
  protected int ccpReqID;

  /** Time that CCP started processing this request */
  protected long startTime;

  /** True if CCP is requesting current set of active replicas for this request. False, otherwise */
  private boolean lookupActives = false;

  /** Number of times this request has initiated lookup actives operation.
   * This could be always zero for some types of requests (namely ADD and REMOVE) that are
   * sent only to replica controllers (and never to active replicas) */
  protected int numLookupActives = 0;


  /***********
   * Fields only for collecting statistics for a request and write log entries
   ***********/

  protected Packet.PacketType requestType;

  /** Time that LNS completed processing this request */
  protected long finishTime =  -1;

  /** Whether requests is finally successful or not. */
  protected boolean success = false;

  // Abstract methods

  /** 
   * Returns the log entry that we will log at local name server
   * @return  */
  public abstract String getLogString();

  /** 
   * Returns the error message to be sent to client if name server returns no response to a request.
   * @return  */
  public abstract JSONObject getErrorMessage();


  // methods that are implemented

  public synchronized String getName() {
    return name;
  }

  public synchronized int getCCPReqID() {
    return ccpReqID;
  }

  public synchronized long getStartTime() {
    return startTime;
  }


  /** 
   * Time duration for which the request was under processing at the local name server.
   * @return 
   */
  public synchronized long getResponseLatency() {
    return finishTime - startTime;
  }

  public synchronized long getFinishTime() {
    return finishTime;
  }

  public synchronized void setFinishTime() {
    this.finishTime = System.currentTimeMillis();
  }

  public synchronized boolean isSuccess() {
    return success;
  }

  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

  public synchronized boolean isLookupActives() {
    return lookupActives;
  }

  /** 
   * Returns true if actives are currently not being requested currently. Otherwise false.
   * @return 
   */
  public synchronized boolean setLookupActives() {
    if (lookupActives) {
      return false;
    } else {
      lookupActives = true;
      numLookupActives += 1;
      return true;
    }
  }

  /** After active replicas are received for this request, reset the lookup actives variables */
  public synchronized void unsetLookupActives() {
    assert lookupActives;
    lookupActives = false;
  }

  public synchronized int getNumLookupActives() {
    return numLookupActives;
  }

}
