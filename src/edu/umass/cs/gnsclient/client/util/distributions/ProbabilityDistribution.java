package edu.umass.cs.gnsclient.client.util.distributions;

/**
 * Created by abhigyan on 3/30/14.
 */
public interface ProbabilityDistribution {

  public double getNextArrivalDelay();

  public double getMean();
}
