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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util.distributions;

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
