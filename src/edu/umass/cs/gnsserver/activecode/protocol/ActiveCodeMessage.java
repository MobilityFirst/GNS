package edu.umass.cs.gnsserver.activecode.protocol;

import java.io.Serializable;

/**
 * Used to pass messages between the main GNS process
 * and active code worker processes
 *
 * @author mbadov
 *
 */
public class ActiveCodeMessage implements Serializable {

  /**
   * Indicates that the worker should shut down
   */
  public boolean shutdown;

  /**
   * Indicates that the active code request finished
   */
  public boolean finished;

  /**
   * Inidicates that the active code request crashed
   */
  public boolean crashed;

  /**
   * The active code params to execute
   */
  public ActiveCodeParams acp;

  /**
   * Stores the result of the active code computation
   */
  public String valuesMapString;

  /**
   * Denotes a request by the worker to perform a query
   */
  public ActiveCodeQueryRequest acqreq;

  /**
   * Returns a query response to the worker
   */
  public ActiveCodeQueryResponse acqresp;

  public boolean isShutdown() {
    return shutdown;
  }

  public void setShutdown(boolean shutdown) {
    this.shutdown = shutdown;
  }

  public boolean isFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public boolean isCrashed() {
    return crashed;
  }

  public void setCrashed(boolean crashed) {
    this.crashed = crashed;
  }

  public ActiveCodeParams getAcp() {
    return acp;
  }

  public void setAcp(ActiveCodeParams acp) {
    this.acp = acp;
  }

  public String getValuesMapString() {
    return valuesMapString;
  }

  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  public ActiveCodeQueryRequest getAcqreq() {
    return acqreq;
  }

  public void setAcqreq(ActiveCodeQueryRequest acqreq) {
    this.acqreq = acqreq;
  }

  public ActiveCodeQueryResponse getAcqresp() {
    return acqresp;
  }

  public void setAcqresp(ActiveCodeQueryResponse acqresp) {
    this.acqresp = acqresp;
  }

}
