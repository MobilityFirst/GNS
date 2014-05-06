package edu.umass.cs.amazontools;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import java.io.IOException;

/**
 *
 * @author westy
 */
public class Amazontool {

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws IOException {
    AWSCredentials credentials = new PropertiesCredentials(
            AWSEC2.class.getResourceAsStream("resources/AwsCredentials.properties"));
    //Create Amazon Client object
    AmazonEC2 ec2 = new AmazonEC2Client(credentials);
    AWSEC2.describeAllEndpoints(ec2);
//    Instance instance = AWSEC2.findInstance(ec2, "i-86983af6");
//    Address elasticIP = AWSEC2.findElasticIP(ec2, "23.21.120.250");
//    System.out.println(instance.getPublicDnsName());
//    System.out.println(elasticIP.getPublicIp());
    //AWSEC2.associateAddress(ec2, "23.21.120.250", instance);
  }
}
