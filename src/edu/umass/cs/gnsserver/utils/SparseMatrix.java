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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.utils;

import java.util.HashMap;

/**
 * Simple implementation of a Sparse Matrix using nested HashMaps.
 *
 * @author westy
 * @param <U>
 * @param <V>
 * @param <T>
 */
public class SparseMatrix<U, V, T> {

  HashMap<U, HashMap<V, T>> rows = new HashMap<>();

  T defaultValue;

  /**
   * Creates a SparseMatrix with a default item value of null.
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

  /**
   * Puts an element into the sparse matrix.
   * 
   * @param i the first index
   * @param j the second index
   * @param t the element to add
   */
  public void put(U i, V j, T t) {
    if (rows.get(i) == null) {
      rows.put(i, new HashMap<V, T>());
    }
    rows.get(i).put(j, t);
  }

  /**
   * Retrieves an element from the sparse matrix.
   * 
   * @param i the first index
   * @param j the second index
   * @return the element
   */
  public T get(U i, V j) {
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
