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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import org.json.JSONObject;

/**
 * Intercessor is an object to which a local name server forward the final response for all requests. These responses
 * are subsequently sent to clients. The interface for an intercessor is given below.
 *
 * The intercessor for the GNS client library is in {@link edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Intercessor}
 *
 */
public interface IntercessorInterface {

  /**
   * Handle packets coming in to the Local Name Server.
   * 
   * @param jsonObject
   */
  public void handleIncomingPacket(JSONObject jsonObject);
}
