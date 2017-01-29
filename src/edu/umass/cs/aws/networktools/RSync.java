
package edu.umass.cs.aws.networktools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


public class RSync {

  private static boolean verbose = true;
  

  public static void upload(String user, String host, File keyFile, String localFile, String remoteFile) {
    try {
    ArrayList<String> command = new ArrayList<>();
    command.add("rsync");
    command.add("-e");
    command.add("ssh -o StrictHostKeyChecking=no -i " + keyFile.getAbsolutePath());
    command.add(localFile);
    command.add(user + "@" + host + ":" + remoteFile);
    if (verbose) {
      System.out.println("rsync command: " + command);
    }
    ProcessBuilder pb = new ProcessBuilder(command);
    Process p = pb.start();
    int val = p.waitFor();
    if (val != 0) {
      throw new RuntimeException("Exception during rsync upload; return code = " + val);
    }
    } catch (IOException | InterruptedException | RuntimeException e) {
       System.out.println("Exception while uploading file:" + e);
    }
  }
  

  public static boolean isVerbose() {
    return verbose;
  }


  public static void setVerbose(boolean verbose) {
    RSync.verbose = verbose;
  }
  

  public static void main(String[] arg) {
    String host = "ec2-23-21-120-250.compute-1.amazonaws.com";
    String localFile = "/Users/westy/Documents/Code/GNS/dist/GNS.jar";
    String remoteFile = "/home/ec2-user/GNStest.jar";
    File keyFile = new File("/Users/westy/aws.pem");
    upload("ec2-user", host, keyFile, localFile, remoteFile);
    System.exit(0);
  }
}
