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
