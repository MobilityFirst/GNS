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
package edu.umass.cs.aws.support;

/**
 *
 * @author westy
 */
public enum RegionRecord {
  
 US_EAST_1("ec2.us-east-1.amazonaws.com", "N. Virginia"),
 US_WEST_1("ec2.us-west-1.amazonaws.com", "N. California"),
 US_WEST_2("ec2.us-west-2.amazonaws.com", "Oregon"),
 EU_WEST_1("ec2.eu-west-1.amazonaws.com", "Ireland"),
 EU_CENTRAL_1("ec2.eu-central-1.amazonaws.com", "Frankfurt"),
 AP_SOUTHEAST_1("ec2.ap-southeast-1.amazonaws.com", "Singapore"),
 AP_SOUTHEAST_2("ec2.ap-southeast-2.amazonaws.com", "Sidney, Australia"),
 AP_NORTHEAST_1("ec2.ap-northeast-1.amazonaws.com", "Tokyo, Japan"),
 SA_EAST_1("ec2.sa-east-1.amazonaws.com", "Sao Paulo, Brazil")
 
 ;
 
 String URL;
 String location;

  private RegionRecord(String name, String location) {
    this.URL = name;
    this.location = location;
  }

  public String getURL() {
    return URL;
  }

  public String getLocation() {
    return location;
  }
  
}

