/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.workloads;

import java.util.Random;

/*
 * This class generates workloads from the Zipf distribution.
 */
public class ZipfWorkloadGenerator {

  /**
   * Random number generator *
   */
  private Random random = new Random(System.currentTimeMillis());

  /**
   * Number of objects *
   */
  private int size;

  /**
   * The value of the exponent characterizing the distribution *
   */
  private double alpha;

  /**
   * Denominator *
   */
  private double denominator = 0.0;

  /**
   *
   * Constructs a ZipfWorkloadGenerator object that generates
   * Zipf distribution workload.
   *
   * @param size Number of objects
   * @param alpha Exponent characterizing the distribution
   *
   */
  public ZipfWorkloadGenerator(int size, double alpha) {
    this.size = size;
    this.alpha = alpha;

    for (int rank = 1; rank <= size; rank++) {
      this.denominator += (1.0d / Math.pow(rank, alpha));
    }
  }

  /**
   *
   * Returns an id of ith most popular name. The frequency of
   * the returned element follows the Zipf distribution.
   *
   * @return Returns an id of the ith most popular name
   *
   */
  public String next() {
    int rank;
    double frequency = 0;
    double dice;

    do {
      rank = random.nextInt(size) + 1;
      frequency = (1.0d / Math.pow(rank, alpha)) / denominator;
      dice = random.nextDouble();
    } while (!(dice < frequency));

    return Integer.toString(rank);
  }

  //Runs faster. Use this
  /**
   *
   * Returns an id of ith most popular name. The frequency of
   * the returned element follows the Zipf distribution.
   *
   * @return Returns an id of the ith most popular name
   *
   */
  public String next2() {
    int rank;
    double prob; //prob is a uniform random number (0 < prob < 1)
    prob = random.nextDouble();

    //Map prob to the rank
    double sumProb = 0.0;
    for (rank = 1; rank <= size; rank++) {
      sumProb = sumProb + (1.0d / Math.pow(rank, alpha)) / denominator;
      if (sumProb >= prob) {
        break;
      }
    }
    return Integer.toString(rank);
  }

  public String inverse() {
    int rank = Integer.parseInt(next2());
    int inverse = size + 1 - rank;
    return Integer.toString(inverse);
  }

  /**
   * Test *
   */
  public static void main(String[] args) {
    double bottom = 0;
    for (double i = 1.0; i <= 5000.0; i++) {
      bottom += (1.0d / Math.pow(i, 0.91));
    }

    double add = 0.0;
    String half_name = null;

    double a = 0;

    for (double i = 1.0; i <= 5000.0; i++) {
      double f = (1.0d / Math.pow(i, 0.91)) / bottom;
      add += f;
      if (half_name == null && add > 0.5) {
        half_name = Double.toString(i);
      }

      if (i == 4187) {
        a = add;
      }

      System.out.println(i + "\t" + f);
    }

    System.out.println("Half: " + half_name);
    System.out.println(a);
  }
}
