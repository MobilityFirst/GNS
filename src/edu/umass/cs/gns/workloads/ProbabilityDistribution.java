package edu.umass.cs.gns.workloads;

/**
 * Created by abhigyan on 3/30/14.
 */
public interface ProbabilityDistribution {

  public double getNextArrivalDelay();

  public double getMean();
}
