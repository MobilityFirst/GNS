/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
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
