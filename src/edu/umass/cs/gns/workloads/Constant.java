package edu.umass.cs.gns.workloads;

/**
 * Implements a fixed value function.
 * Created by abhigyan on 3/30/14.
 */
public class Constant implements ProbabilityDistribution {

  private double mean;

  public Constant(double mean) {
    this.mean = mean;
  }
  @Override
  public double getNextArrivalDelay() {
    return mean;
  }

  @Override
  public double getMean() {
    return mean;
  }
}
