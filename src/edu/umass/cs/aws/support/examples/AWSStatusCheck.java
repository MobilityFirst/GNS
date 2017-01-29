
package edu.umass.cs.aws.support.examples;


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


public class AWSStatusCheck {

  private static final String FILESEPARATOR = System.getProperty("file.separator");

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


  public static void main(String[] args) throws Exception {

    init();


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


      try {
        List<Bucket> buckets = s3.listBuckets();

        long totalSize = 0;
        int totalItems = 0;
        for (Bucket bucket : buckets) {

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

        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
      } catch (AmazonClientException ace) {

        System.out.println("Error Message: " + ace.getMessage());
      }
    }
  }
}
