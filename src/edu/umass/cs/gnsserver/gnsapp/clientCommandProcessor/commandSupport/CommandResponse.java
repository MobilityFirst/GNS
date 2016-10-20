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

import edu.umass.cs.gnscommon.ResponseCode;

/**
 * Encapsulates the response string and {@link ResponseCode} that we pass back to the client.
 */
public class CommandResponse {

  /**
   * Value returned.
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error. Can be null.
   */
  private final ResponseCode errorCode;

  /**
   * Create a command response object from a return value with an error code.
   *
   * @param errorCode
   * @param returnValue
   *
   */
  // Full returnValue strings (second arg) are used by the HTTP server and
  // also retain backward compatibility with older clients.
  public CommandResponse(ResponseCode errorCode, String returnValue) {
    this.returnValue = returnValue;
    this.errorCode = errorCode;
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
   * @return a {@link ResponseCode}
   */
  public ResponseCode getExceptionOrErrorCode() {
    return errorCode;
  }
}
