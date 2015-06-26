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

