/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.packet;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Holds values for the GNS. This is the class that is used to store values in the
 * in-memory side of the key value store. Put another way, this is the VALUE part of the
 * key / value store when we are manipulating the key value store in memory.
 * 
 * This is also used for transmitting values back to the client.
 * 
 * An important thing to keep in mind is that all values in key / values are lists.
 * If you're storing a single value as the value of the key, that is going to be stored
 * as a one element list.
 * 
 * @author westy
 */
public class QueryResultValue extends CopyOnWriteArrayList<String> {

  public QueryResultValue(Collection<? extends String> c) {
    super(c);
  }

  public QueryResultValue() {
  }
  
}
