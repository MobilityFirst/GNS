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
@SuppressWarnings("serial")
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
   * Inidicates that the active code execution error
   */
  public String error;

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
  
  public String toString(){
	  return "{valuesMapString:"+(valuesMapString==null?"[NULL]":valuesMapString)+",parameter:"+(acp==null?"[NULL]":acp)+"}";
  }
  
  /**
   * shutdown getter
   * @return true if has been shutdown
   */
  public boolean isShutdown() {
    return shutdown;
  }

  /**
   * shutdown setter
   * @param shutdown
   */
  public void setShutdown(boolean shutdown) {
    this.shutdown = shutdown;
  }

  /**
   * finished getter
   * @return true if it's finished
   */
  public boolean isFinished() {
    return finished;
  }

  /**
   * finished setter
   * @param finished
   */
  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  /**
   * parameter getter
   * @return active code params
   */
  public ActiveCodeParams getAcp() {
    return acp;
  }

  /**
   * params setter
   * @param acp
   */
  public void setAcp(ActiveCodeParams acp) {
    this.acp = acp;
  }

  /**
   * result getter
   * @return result
   */
  public String getValuesMapString() {
    return valuesMapString;
  }

  /**
   * result setter
   * @param valuesMapString
   */
  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  /**
   * request getter
   * @return request
   */
  public ActiveCodeQueryRequest getAcqreq() {
    return acqreq;
  }

  /**
   * request setter
   * @param acqreq
   */
  public void setAcqreq(ActiveCodeQueryRequest acqreq) {
    this.acqreq = acqreq;
  }

  /**
   * response getter
   * @return response
   */
  public ActiveCodeQueryResponse getAcqresp() {
    return acqresp;
  }

  /**
   * response setter
   * @param acqresp
   */
  public void setAcqresp(ActiveCodeQueryResponse acqresp) {
    this.acqresp = acqresp;
  }
  
  /**
   * @return true if error message is not null
   */
  public boolean isCrashed(){
	  boolean crashed = false;
	  if (this.error != null){
		  crashed = true;
	  }
	  return crashed;
  }
  
  /**
   * @param errMsg
   */
  public void setCrashed(String errMsg){
	  error = errMsg;
  }
}
