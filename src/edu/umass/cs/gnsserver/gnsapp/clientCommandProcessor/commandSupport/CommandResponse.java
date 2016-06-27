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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSResponseCode;

/**
 * Encapsulates the response values and instrumentation that we pass back to the client.
 *
 * @param <NodeIDType>
 */
public class CommandResponse<NodeIDType> {

  /**
   * Value returned.
   */
  private String returnValue;
  /**
   * Indicates if the response is an error. Can be null.
   */
  private GNSResponseCode errorCode;

  /**
   * Create a command response object from a return value with an error code.
   *
   * @param returnValue
   * @param errorCode
   */
  public CommandResponse(String returnValue, GNSResponseCode errorCode) {
    this.returnValue = returnValue;
    this.errorCode = errorCode;
  }

  /**
   * Create a command response object from a return value with no error.
   *
   * @param returnValue
   */
  public CommandResponse(String returnValue) {
    this(returnValue, GNSResponseCode.NO_ERROR);
  }

  /**
   * Gets the return value.
   *
   * @return a string
   */
  public String getReturnValue() {
    return returnValue;
  }

  /**
   * Gets the error code.
   *
   * @return a {@link GNSResponseCode}
   */
  public GNSResponseCode getExceptionOrErrorCode() {
    return errorCode;
  }
}
