package edu.umass.cs.gnrs.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gnrs.main.StartNameServer;

/*************************************************************
 * This class implements the moving average
 * 
 * @author Hardeep Uppal
 ************************************************************/
public class MovingAverage {

  /** FIFO Queue for calculating moving average **/
  private Queue<Integer> window;
  /** Size of the moving average window **/
  private final int WINDOW_SIZE;
  /** Sum of all numbers in the window **/
  private double sum;

  /*************************************************************
   * Constructs a new MovingAverage object with the specified 
   * window size
   * @param windowSize Window size 
   ************************************************************/
  public MovingAverage(int windowSize) {
    this.window = new LinkedList<Integer>();
    this.WINDOW_SIZE = windowSize;
    this.sum = 0;
  }
  
  /*************************************************************
   * Constructs a new MovingAverage from a JSON Array with integers 
   ************************************************************/
  public MovingAverage(JSONArray json, int windowSize) {
    try {
      this.window = new LinkedList<Integer>(JSONUtils.JSONArrayToArrayListInteger(json));
    } catch (JSONException e) {
      this.window = new LinkedList<Integer>();
    }
    this.WINDOW_SIZE = windowSize;
    this.sum = 0;
    for (Integer x : this.window) {
      this.sum = this.sum + x;
    }
  }
  
  public JSONArray toJSONArray () {
    return new JSONArray(window);
  }

  /*************************************************************
   * Adds a new number to the moving average window.
   * When the window is full, the oldest entry (head) in the 
   * queue is removed and the new number is added to the tail.
   * @param num New number added to the window
   ************************************************************/
  public void add(int num) {
    sum += num;
    window.add(num);
    //Removed head when window is full.
    if (window.size() > WINDOW_SIZE) {
      sum -= window.remove();
    }
  }

  /*************************************************************
   * Returns the moving average.<br/>
   * Returns 0 if the window is empty.
   ************************************************************/
  public double getAverage() {
    return (window.isEmpty()) ? 0 : (sum / window.size());
  }

  /*************************************************************
   * Returns the median value in window.<br/>
   * Returns 0 if the window is empty.
   ************************************************************/
  public double getMedian() {
	  if  (window.isEmpty()) return 0;
	  int[] sortedList = new int[window.size()];
	  int i = 0; 
	  for (int x: window) {
		  sortedList[i] = x;
		  i++;
	  }
	  Arrays.sort(sortedList);
	  return sortedList[sortedList.length / 2];
	  
  }

  /*************************************************************
   * Returns the String representation of this object
   ************************************************************/
  public String toString() {
    return "Window:" + window.toString() + " Size:" + window.size() + " Sum:" + sum + " Avg:" + getAverage();
  }

  /** Test **/
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
  }
}
