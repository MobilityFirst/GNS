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

import java.util.ArrayList;
import java.util.Collections;

/**
 * Simple stats for short lists - not designed to be particularly fast or efficient.
 * 
 * @author westy
 */
public class Stats {

  ArrayList<Double> data;
  double size;

  /**
   *
   * @param data
   */
  public Stats(ArrayList<Double> data) {
    this.data = data;
    size = data.size();
  }
  
  /**
   * Return the size.
   * 
   * @return an int
   */
  public int getN() {
    return (int) size;
  }

  /**
   * Return the mean.
   * 
   * @return a double
   */
  public double getMean() {
    double sum = 0.0;
    for (double a : data) {
      sum += a;
    }
    return sum / size;
  }

  /**
   * Return the variance.
   * 
   * @return a double
   */
  public double getVariance() {
    double mean = getMean();
    double temp = 0;
    for (double a : data) {
      temp += (mean - a) * (mean - a);
    }
    return temp / size;
  }

  /**
   * Return the standard deviation.
   * 
   * @return a double
   */
  public double getStdDev() {
    return Math.sqrt(getVariance());
  }

  /**
   * Return the median.
   * 
   * @return a double
   */
  public double median() {
    ArrayList<Double> copy = new ArrayList<Double>(data);
    Collections.sort(data);

    if (copy.size() % 2 == 0) {
      return (copy.get((copy.size() / 2) - 1) + copy.get(copy.size() / 2)) / 2.0;
    } else {
      return copy.get(copy.size() / 2);
    }
  }
}
