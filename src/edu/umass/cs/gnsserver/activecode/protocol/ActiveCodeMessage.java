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

  private static final long serialVersionUID = 2326392043474125897L;

  /**
   * Indicates that the worker should shut down
   */
  private boolean shutdown;

  /**
   * Indicates that the active code request finished
   */
  private boolean finished;

  /**
   * Inidicates that the active code request crashed
   */
  private boolean crashed;

  /**
   * The active code params to execute
   */
  private ActiveCodeParams acp;

  /**
   * Stores the result of the active code computation
   */
  private String valuesMapString;

  /**
   * Denotes a request by the worker to perform a query
   */
  private ActiveCodeQueryRequest acqreq;

  /**
   * Returns a query response to the worker
   */
  private ActiveCodeQueryResponse acqresp;

  /**
   * shutdown getter
   *
   * @return true if has been shutdown
   */
  public boolean isShutdown() {
    return shutdown;
  }

  /**
   * shutdown setter
   *
   * @param shutdown
   */
  public void setShutdown(boolean shutdown) {
    this.shutdown = shutdown;
  }

  /**
   * finished getter
   *
   * @return true if it's finished
   */
  public boolean isFinished() {
    return finished;
  }

  /**
   * finished setter
   *
   * @param finished
   */
  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  /**
   * crashed getter
   *
   * @return true if worker crashed
   */
  public boolean isCrashed() {
    return crashed;
  }

  /**
   * crashed setter
   *
   * @param crashed
   */
  public void setCrashed(boolean crashed) {
    this.crashed = crashed;
  }

  /**
   * parameter getter
   *
   * @return active code params
   */
  public ActiveCodeParams getAcp() {
    return acp;
  }

  /**
   * params setter
   *
   * @param acp
   */
  public void setAcp(ActiveCodeParams acp) {
    this.acp = acp;
  }

  /**
   * result getter
   *
   * @return result
   */
  public String getValuesMapString() {
    return valuesMapString;
  }

  /**
   * result setter
   *
   * @param valuesMapString
   */
  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  /**
   * request getter
   *
   * @return request
   */
  public ActiveCodeQueryRequest getAcqreq() {
    return acqreq;
  }

  /**
   * request setter
   *
   * @param acqreq
   */
  public void setAcqreq(ActiveCodeQueryRequest acqreq) {
    this.acqreq = acqreq;
  }

  /**
   * response getter
   *
   * @return response
   */
  public ActiveCodeQueryResponse getAcqresp() {
    return acqresp;
  }

  /**
   * response setter
   *
   * @param acqresp
   */
  public void setAcqresp(ActiveCodeQueryResponse acqresp) {
    this.acqresp = acqresp;
  }

}
