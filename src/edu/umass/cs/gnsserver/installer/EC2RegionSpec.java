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
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.aws.support.RegionRecord;

/**
 * Represents EC2 hosts that will be created.
 * 
 * @author westy
 */
public class EC2RegionSpec {

  private RegionRecord region;
  private int count;
  private String ip;

  /**
   * Create a EC2RegionSpec instance.
   * 
   * @param region
   * @param count
   * @param ip
   */
  public EC2RegionSpec(RegionRecord region, int count, String ip) {
    this.region = region;
    this.count = count;
    this.ip = ip;
  }

  /**
   * Returns the region.
   *
   * @return the region
   */
  public RegionRecord getRegion() {
    return region;
  }

  /**
   * Returns the number of hosts to create.
   *
   * @return the number of hosts
   */
  public int getCount() {
    return count;
  }

  /**
   * Returns the IP address to use.
   *
   * @return the IP address
   */
  public String getIp() {
    return ip;
  }

  @Override
  public String toString() {
    return "RegionSpec{" + "region=" + region + ", count=" + count + ", ip=" + ip + '}';
  }
}
