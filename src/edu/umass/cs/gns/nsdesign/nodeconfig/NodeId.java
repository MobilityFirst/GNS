/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

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

  @Override
  public String toString() {
    return id.toString();
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

  @Override
  public int compareTo(NodeId<NodeItemType> that) {
    return ((Comparable) this.id).compareTo(that.id);
  }

}
