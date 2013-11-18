/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.aws;

import edu.umass.cs.amazontools.RegionRecord;

/**
 *
 * @author westy
 */
public class EC2RegionSpec {

    private RegionRecord region;
    private int count;
    private String ip;

    public EC2RegionSpec(RegionRecord region, int count, String ip) {
      this.region = region;
      this.count = count;
      this.ip = ip;
    }

    public RegionRecord getRegion() {
      return region;
    }

    public int getCount() {
      return count;
    }

    public String getIp() {
      return ip;
    }

    @Override
    public String toString() {
      return "RegionSpec{" + "region=" + region + ", count=" + count + ", ip=" + ip + '}';
    }
  }
