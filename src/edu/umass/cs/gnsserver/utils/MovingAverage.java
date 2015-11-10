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

import org.json.JSONArray;
import org.json.JSONException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

/**
 *
 * Implements the moving average.
 *
 */
public class MovingAverage {

  /**
   * FIFO Queue for calculating moving average *
   */
  private final Queue<Integer> window;
  /**
   * Size of the moving average window *
   */
  private final int WINDOW_SIZE;

  /**
   * Sum of all numbers in the window *
   */
  private double sum;

  /**
   * ***********************************************************
   * Constructs a new MovingAverage object with the specified
   * window size
   *
   * @param windowSize Window size
   ***********************************************************
   */
  public MovingAverage(int windowSize) {
    this.window = new LinkedList<Integer>();
    this.WINDOW_SIZE = windowSize;
    this.sum = 0;
  }

  /**
   * ***********************************************************
   * Constructs a new MovingAverage from a JSON Array with integers
   ***********************************************************
   * @param json
   * @param windowSize
   * @throws org.json.JSONException
   */
  public MovingAverage(JSONArray json, int windowSize) throws JSONException {
    this.window = new LinkedList<Integer>(JSONUtils.JSONArrayToArrayListInteger(json));
    this.WINDOW_SIZE = windowSize;
    this.sum = 0;
    for (Integer x : this.window) {
      this.sum = this.sum + x;
    }
  }

  /**
   *
   * @param values
   * @param windowSize
   */
  public MovingAverage(ArrayList<Integer> values, int windowSize) {
    this.window = new LinkedList<Integer>(values);
    this.WINDOW_SIZE = windowSize;
    this.sum = 0;
    for (Integer x : this.window) {
      this.sum = this.sum + x;
    }
  }

  /**
   * The sum.
   * 
   * @return the sum
   */
  public double getSum() {
    return sum;
  }

  /**
   * Return the all the values in the window as a JSON Array.
   * 
   * @return all the values in the window as a JSON Array
   */
  public JSONArray toJSONArray() {
    return new JSONArray(window);
  }

  /**
   * Return the all the values in the window as an arrayList.
   * 
   * @return all the values in the window as an arrayList.
   */
  public ArrayList<Integer> toArrayList() {
    return new ArrayList<Integer>(this.window);
  }

  /**
   * ***********************************************************
   * Adds a new number to the moving average window.
   * When the window is full, the oldest entry (head) in the
   * queue is removed and the new number is added to the tail.
   *
   * @param num New number added to the window
   ***********************************************************
   */
  public synchronized void add(int num) {
    sum += num;
    window.add(num);
    //Removed head when window is full.
    if (window.size() > WINDOW_SIZE) {
      sum -= window.remove();
    }
  }

  /**
   * Returns the moving average.<br>
   * Returns 0 if the window is empty.
   * @return the moving average
   */
  public synchronized double getAverage() {
    return (window.isEmpty()) ? 0 : (sum / window.size());
  }

  /**
   * Returns the median value in window.<br>
   * Returns 0 if the window is empty.
   * @return the median value
   */
  public synchronized double getMedian() {
    if (window.isEmpty()) {
      return 0;
    }
    int[] sortedList = new int[window.size()];
    int i = 0;
    for (int x : window) {
      sortedList[i] = x;
      i++;
    }
    Arrays.sort(sortedList);
    return sortedList[sortedList.length / 2];

  }

  @Override
  public String toString() {
    return "Window:" + window.toString() + " Size:" + window.size() + " Sum:" + sum + " Avg:" + getAverage();
  }

  /**
   * The main routine. For testing only.
   *
   * @param args *
   */
  public static void main(String[] args) {
    int[] testData = {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
    int[] windowSizes = {3, 5};
    System.out.println("TESTING EMPTY CREATE");
    for (int windSize : windowSizes) {
      MovingAverage ma = new MovingAverage(windSize);
      System.out.println(ma.getAverage());
      for (int x : testData) {
        ma.add(x);
        System.out.println("Next number = " + x + ", MA = " + ma.getAverage());
        System.out.println(ma.toString());
      }
      System.out.println();
    }
    System.out.println("TESTING FULL CREATE"); // Westy
    try {
      MovingAverage ma = new MovingAverage(new JSONArray(testData), testData.length);
      System.out.println(ma.toString());
    } catch (JSONException e) {
      System.out.println("Error: " + e); // Westy
    }
    System.out.println("CHECKING FOR THREAD SAFETY. THIS WILL RUN FOREVER.");
    final MovingAverage ma1 = new MovingAverage(20);
    (new Thread() {
      @Override
      public void run() {
        Random random = new Random();
        while (true) {
          ma1.add(random.nextInt(100));
        }
      }
    }).start();
    (new Thread() {
      @Override
      public void run() {
        Random random = new Random();
        while (true) {
          ma1.add(random.nextInt(100));
        }
      }
    }).start();
    (new Thread() {
      @Override
      public void run() {
        Random random = new Random();
        while (true) {
          ma1.add(random.nextInt(100));
        }
      }
    }).start();
    (new Thread() {
      @Override
      public void run() {
        Random random = new Random();
        while (true) {
          ma1.add(random.nextInt(100));
        }
      }
    }).start();
    (new Thread() {
      @Override
      public void run() {
        int x = 0;
        while (true) {
          ma1.getAverage();
          ma1.getMedian();
        }
      }
    }).start();
  }
}
