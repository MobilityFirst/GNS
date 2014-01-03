package edu.umass.cs.gns.aws;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;

/**
 * Created by abhigyan on 1/1/14.
 */
public class S3JarUpload {
  private static String bucketName     = "auspice-gns";
  private static String keyName        = "*** Provide key ***";
  private static String uploadFileName = "*** Provide file name ***";

  public static void main(String[] args) throws IOException {
    AmazonS3 s3client = new AmazonS3Client(new PropertiesCredentials(
            S3JarUpload.class.getResourceAsStream(
                    "AwsCredentials.properties")));
    try {
      System.out.println("Uploading a new object to S3 from a file\n");
      File file = new File(uploadFileName);
      s3client.putObject(new PutObjectRequest(
              bucketName, keyName, file));

    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which " +
              "means your request made it " +
              "to Amazon S3, but was rejected with an error response" +
              " for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which " +
              "means the client encountered " +
              "an internal error while trying to " +
              "communicate with S3, " +
              "such as not being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }
}
