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
public class NodeId<NodeItemType> implements Comparable<NodeId<NodeItemType>>{

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
    GNS.getLogger().warning("toString called in " + Thread.currentThread().getStackTrace()[2].toString() + " use get() instead for now.");
    return id.toString();
  }

  @Override
  public int compareTo(NodeId<NodeItemType> o) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
