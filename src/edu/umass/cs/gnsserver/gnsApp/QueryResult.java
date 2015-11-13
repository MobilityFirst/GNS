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
package edu.umass.cs.gnsserver.gnsApp;


import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.InternalField;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Either a ValuesMap or an Error. Used to represent values returned
 * from a DNS query. See also {@link NSResponseCode} which is used to
 * represent errors in this class.
 * 
 * Also has some instrumentation for round trip times and what server responded.
 * 
 * @author westy
 * @param <NodeIDType>
 */
public class QueryResult<NodeIDType> implements Serializable{

  /**
   * Set if the response is not an error.
   */
  private ValuesMap valuesMap = null;
  /**
   * Set if the response is an error.
   */
  private NSResponseCode errorCode = null;
  /** 
   * Instrumentation - records the time between the LNS sending the request to the NS and the return message.
   */
  private long roundTripTime; // how long this query took
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private final NodeIDType responder;

  /**
   * Creates a "normal" (non-error) QueryResult.
   * 
   * @param valuesMap 
   * @param responder 
   */
  public QueryResult(ValuesMap valuesMap, NodeIDType responder) {
    this.valuesMap = valuesMap;
    this.responder = responder;
  }

  /**
   * Creates an error QueryResult.
   * 
   * @param errorCode 
   * @param responder 
   */
  public QueryResult(NSResponseCode errorCode, NodeIDType responder) {
    this.errorCode = errorCode;
    this.responder = responder;
  }

  /**
   * Gets the ValuesMap of this QueryResult.
   * 
   * @return 
   */
  public ValuesMap getValuesMap() {
    return valuesMap;
  }

  /**
   * Gets the ValuesMap, but scrubs any internal fields first.
   * @return 
   */
  public ValuesMap getValuesMapSansInternalFields() {
    ValuesMap copy = new ValuesMap(valuesMap);
    removeInternalFields(copy);
    return copy;
  }

  /**
   * Gets one ResultValue from the ValuesMap.
   * 
   * @param key
   * @return 
   */
  public ResultValue getArray(String key) {
    if (valuesMap != null) {
      return valuesMap.getAsArray(key);
    } else {
      return null;
    }
  }

  /**
   * Remove any keys / value pairs used internally by the GNS.
   * 
   * @param valuesMap
   * @return 
   */
  private static ValuesMap removeInternalFields(ValuesMap valuesMap) {
    Iterator<?> keyIter = valuesMap.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      if (InternalField.isInternalField(key)) {
        keyIter.remove();
      }
    }
    return valuesMap;
  }

  /**
   * Returns the error code. Can be null.
   * 
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  /**
   * Does this QueryResult represent an error result.
   * 
   * @return true if it is an error
   */
  public boolean isError() {
    return this.errorCode != null;
  }

  /** 
   * Instrumentation - holds the time between the LNS sending the request to the NS and the return message
   * being received.
   * @return 
   */
  public long getRoundTripTime() {
    return roundTripTime;
  }

  /** 
   * Instrumentation - holds the time between the LNS sending the request to the NS and the return message
   * being received.
   * @param roundTripTime
   */
  public void setRoundTripTime(long roundTripTime) {
    this.roundTripTime = roundTripTime;
  }

  /**
   * Returns the responder. Might be -1 if missing.
   * 
   * @return a node id
   */
  public NodeIDType getResponder() {
    return responder;
  }

  @Override
  public String toString() {
    return "QueryResult{" + "valuesMap=" + valuesMap + ", errorCode=" + errorCode + ", roundTripTime=" + roundTripTime + ", responder=" + responder + '}';
  }
  
//  public String toReasonableString() {
//    return "QueryResult{" + "valuesMap=" + valuesMap.toString() 
//    return "QueryResult{" + "valuesMap=" + valuesMap.toReasonableString() 
//            + ", errorCode=" + errorCode + ", roundTripTime=" + roundTripTime 
//            + ", responder=" + responder + '}';
//  }

}
