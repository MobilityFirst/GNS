/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

import edu.umass.cs.gns.main.GNS;
import java.util.Objects;

/**
 *
 * @author westy
 * @param <NodeItemType>
 */
public class NodeId<NodeItemType> implements Comparable<NodeId<NodeItemType>> {

  final NodeItemType id;

  public NodeId(NodeItemType id) {
    this.id = id;
  }

  public NodeId(Integer string) {
    this.id = (NodeItemType) Integer.toString(string);
  }

  public NodeItemType get() {
    return id;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 97 * hash + Objects.hashCode(this.id);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final NodeId<?> other = (NodeId<?>) obj;

    return this.id.equals(other.id);
  }

  // so we don't miss any places for now
  @Override
  public String toString() {
    GNS.getLogger().warning("***** toString called in \n   " + stackTraceToString() + " use get() instead for now.");
    return id.toString();
  }

  // cute little hack to show us where
  private String stackTraceToString() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    return (stackTrace.length > 2 ? stackTrace[2].toString() + "\n   " : "")
            + (stackTrace.length > 3 ? stackTrace[3].toString() + "\n   " : "")
            + (stackTrace.length > 4 ? stackTrace[4].toString() + "\n   " : "")
            + (stackTrace.length > 5 ? stackTrace[5].toString() + "\n   " : "")
            + (stackTrace.length > 6 ? stackTrace[6].toString() + "\n   " : "")
            + (stackTrace.length > 7 ? stackTrace[7].toString() + "\n   " : "")
            + (stackTrace.length > 8 ? stackTrace[8].toString() + "\n" : "");
  }

  
  @Override
  public int compareTo(NodeId<NodeItemType> that) {
     return ((Comparable)this.id).compareTo(that.id);
  }

}
