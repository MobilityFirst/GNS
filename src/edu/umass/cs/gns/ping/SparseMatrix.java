/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import java.util.HashMap;

/**
 * Implementation of a Spare Matrix.
 *
 * @author westy
 */
public class SparseMatrix<T> {

  HashMap<Integer, HashMap<Integer, T>> rows = new HashMap<Integer, HashMap<Integer, T>>();

  T defaultValue;

  /**
   * * Creates a SparseMatrix with a default item value of null.
   */
  public SparseMatrix() {
    this(null);
  }

  /**
   * Creates a SparseMatrix with a default item value of defaultValue.
   *
   * @param defaultValue
   */
  public SparseMatrix(T defaultValue) {
    this.defaultValue = defaultValue;
  }

  public void put(int i, int j, T t) {
    if (rows.get(i) == null) {
      rows.put(i, new HashMap<Integer, T>());
    }
    rows.get(i).put(j, t);
  }

  public T get(int i, int j) {
    if (rows.get(i) == null) {
      return this.defaultValue;
    } else {
      T result = rows.get(i).get(j);
      if (result != null) {
        return result;
      } else {
        return this.defaultValue;
      }
    }
  }

  @Override
  public String toString() {
    return "SparseMatrix{" + "rows=" + rows + '}';
  }

}
