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
package edu.umass.cs.gnsserver.activecode.worker;

import java.io.BufferedReader;
import java.io.PrintWriter;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.utils.ValuesMap;

public class ActiveCodeGuidQuerier {

  private PrintWriter out;
  private BufferedReader in;

  public ActiveCodeGuidQuerier(BufferedReader in, PrintWriter out) {
    this.out = out;
    this.in = in;
  }

  /**
   * Queries (read or write) a guid
   *
   * @param acqreq the request params
   * @return the response
   */
  private ActiveCodeQueryResponse queryGuid(ActiveCodeQueryRequest acqreq) {
    try {
      ActiveCodeMessage acm = new ActiveCodeMessage();
      acm.setAcqreq(acqreq);
      // Send off the query request
      ActiveCodeUtils.sendMessage(out, acm);

      // Wait for a response
      ActiveCodeMessage acmqr = ActiveCodeUtils.getMessage(in);
      return acmqr.getAcqresp();

    } catch (Exception e) {
      e.printStackTrace();
    }
    // Return an empty response to designate failure
    return new ActiveCodeQueryResponse();
  }

  /**
   * Reads a guid by passing the query on to the GNS process
   *
   * @param guid the guid
   * @param field the field
   * @return the ValuesMap response
   */
  public ValuesMap readGuid(String guid, String field) {
    ActiveCodeQueryRequest acqreq = new ActiveCodeQueryRequest(guid, field, null, "read");
    ActiveCodeQueryResponse acqresp = queryGuid(acqreq);

    ValuesMap vm = null;

    try {
      vm = new ValuesMap(new JSONObject(acqresp.getValuesMapString()));
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return vm;
  }

  /**
   * Writes to a guid by passing the query on to the GNS process
   * (Only local guid currently supported).
   *
   * @param guid the guid
   * @param field the field
   * @param newValue the new values as a object
   * @return whether or not the write succeeded
   */
  public boolean writeGuid(String guid, String field, Object newValue) {
    try {
      if (!(newValue instanceof ValuesMap)) {
        ValuesMap valuesMap = new ValuesMap();
        valuesMap.put(field, newValue);
        newValue = valuesMap;
      }
      ActiveCodeQueryRequest acqreq = new ActiveCodeQueryRequest(guid, field, newValue.toString(), "write");
      ActiveCodeQueryResponse acqresp = queryGuid(acqreq);
      return acqresp.isSuccess();
    } catch (JSONException e) {
      e.printStackTrace();
      return false;
    }
  }
}
