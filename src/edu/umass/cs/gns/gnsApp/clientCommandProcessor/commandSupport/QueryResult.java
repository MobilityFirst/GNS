/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;


import edu.umass.cs.gns.utils.ResultValue;
import edu.umass.cs.gns.utils.ValuesMap;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Either a ValuesMap or an Error. Used to represent values returned
 * from a DNS query.
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
//  /**
//   * Database lookup time instrumentation
//   */
//  private final int lookupTime;
  /**
   * Creates a "normal" (non-error) QueryResult.
   * 
   * @param valuesMap 
   * @param responder 
   */
  public QueryResult(ValuesMap valuesMap, NodeIDType responder) {
    this.valuesMap = valuesMap;
    this.responder = responder;
    //this.lookupTime = lookupTime;
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
    //this.lookupTime = lookupTime;
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
    //Iterator<String> keyIter = newContent.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      if (InternalField.isInternalField(key)) {
        keyIter.remove();
        //valuesMap.remove(key);
      }
    }

    return valuesMap;
  }

  /**
   * Returns the error code. Can be null.
   * 
   * @return
   */
  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  /**
   * Does this QueryResult represent an error result.
   * 
   * @return 
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
   * @return
   */
  public NodeIDType getResponder() {
    return responder;
  }

//  public int getLookupTime() {
//    return lookupTime;
//  }

  @Override
  public String toString() {
    return "QueryResult{" + "valuesMap=" + valuesMap + ", errorCode=" + errorCode + ", roundTripTime=" + roundTripTime + ", responder=" + responder + '}';
  }

}
