/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.packet;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

/**
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
