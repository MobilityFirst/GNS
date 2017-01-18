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
package edu.umass.cs.aws.support.examples;

/*
 * Copyright 2010-2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author westy
 */
public class AWSStatusCheck {

  private static final String FILESEPARATOR = System.getProperty("file.separator");
  /*
   * Important: Be sure to fill in your AWS access credentials in the AwsCredentials.properties
   * file before you try to run this.
   */
  private static final String CREDENTIALSFILE = System.getProperty("user.home") + FILESEPARATOR + "AwsCredentials.properties";

  private static AmazonEC2 ec2;
  private static AmazonS3 s3;
  private static AmazonSimpleDB sdb;

  private static void init() throws Exception {
    AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));

    ec2 = new AmazonEC2Client(credentials);
    s3 = new AmazonS3Client(credentials);
    sdb = new AmazonSimpleDBClient(credentials);
  }
  private static ArrayList<String> endpoints
          = new ArrayList<>(Arrays.asList(
                  "ec2.us-east-1.amazonaws.com",
                  "ec2.us-west-1.amazonaws.com",
                  "ec2.us-west-2.amazonaws.com",
                  "ec2.eu-west-1.amazonaws.com",
                  "ec2.eu-west-2.amazonaws.com",
                  "ec2.ap-southeast-1.amazonaws.com",
                  "ec2.ap-northeast-1.amazonaws.com",
                  "ec2.sa-east-1.amazonaws.com"));

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    init();

    /*
     * Amazon EC2
     */
    for (String endpoint : endpoints) {
      try {
        ec2.setEndpoint(endpoint);
        System.out.println("**** Endpoint: " + endpoint);
        DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
        System.out.println("You have access to " + availabilityZonesResult.getAvailabilityZones().size()
                + " Availability Zones.");
        for (AvailabilityZone zone : availabilityZonesResult.getAvailabilityZones()) {
          System.out.println(zone.getZoneName());
        }

        DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();
        Set<Instance> instances = new HashSet<Instance>();

        System.out.println("Instances: ");
        for (Reservation reservation : reservations) {
          for (Instance instance : reservation.getInstances()) {
            instances.add(instance);
            System.out.println(instance.getPublicDnsName() + " is " + instance.getState().getName());
          }
        }

        System.out.println("Security groups: ");
        DescribeSecurityGroupsResult describeSecurityGroupsResult = ec2.describeSecurityGroups();
        for (SecurityGroup securityGroup : describeSecurityGroupsResult.getSecurityGroups()) {
          System.out.println(securityGroup.getGroupName());
        }

        //System.out.println("You have " + instances.size() + " Amazon EC2 instance(s) running.");
      } catch (AmazonServiceException ase) {
        System.out.println("Caught Exception: " + ase.getMessage());
        System.out.println("Reponse Status Code: " + ase.getStatusCode());
        System.out.println("Error Code: " + ase.getErrorCode());
        System.out.println("Request ID: " + ase.getRequestId());
      }

      /*
       * Amazon SimpleDB
       *
       */
      try {
        ListDomainsRequest sdbRequest = new ListDomainsRequest().withMaxNumberOfDomains(100);
        ListDomainsResult sdbResult = sdb.listDomains(sdbRequest);

        int totalItems = 0;
        for (String domainName : sdbResult.getDomainNames()) {
          DomainMetadataRequest metadataRequest = new DomainMetadataRequest().withDomainName(domainName);
          DomainMetadataResult domainMetadata = sdb.domainMetadata(metadataRequest);
          totalItems += domainMetadata.getItemCount();
        }

        System.out.println("You have " + sdbResult.getDomainNames().size() + " Amazon SimpleDB domain(s)"
                + "containing a total of " + totalItems + " items.");
      } catch (AmazonServiceException ase) {
        System.out.println("Caught Exception: " + ase.getMessage());
        System.out.println("Reponse Status Code: " + ase.getStatusCode());
        System.out.println("Error Code: " + ase.getErrorCode());
        System.out.println("Request ID: " + ase.getRequestId());
      }

      /*
       * Amazon S3
       *.
       */
      try {
        List<Bucket> buckets = s3.listBuckets();

        long totalSize = 0;
        int totalItems = 0;
        for (Bucket bucket : buckets) {
          /*
           * In order to save bandwidth, an S3 object listing does not
           * contain every object in the bucket; after a certain point the
           * S3ObjectListing is truncated, and further pages must be
           * obtained with the AmazonS3Client.listNextBatchOfObjects()
           * method.
           */
          ObjectListing objects = s3.listObjects(bucket.getName());
          do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
              totalSize += objectSummary.getSize();
              totalItems++;
            }
            objects = s3.listNextBatchOfObjects(objects);
          } while (objects.isTruncated());
        }

        System.out.println("You have " + buckets.size() + " Amazon S3 bucket(s), "
                + "containing " + totalItems + " objects with a total size of " + totalSize + " bytes.");
      } catch (AmazonServiceException ase) {
        /*
         * AmazonServiceExceptions represent an error response from an AWS
         * services, i.e. your request made it to AWS, but the AWS service
         * either found it invalid or encountered an error trying to execute
         * it.
         */
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
      } catch (AmazonClientException ace) {
        /*
         * AmazonClientExceptions represent an error that occurred inside
         * the client on the local host, either while trying to send the
         * request to AWS or interpret the response. For example, if no
         * network connection is available, the client won't be able to
         * connect to AWS to execute a request and will throw an
         * AmazonClientException.
         */
        System.out.println("Error Message: " + ace.getMessage());
      }
    }
  }
}
