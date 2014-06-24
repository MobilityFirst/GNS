/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.aws.networktools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 *
 * @author westy
 */
public class RSync {

  private static final boolean verbose = true;

  public static void upload(String user, String host, File keyFile, String fileToTransfer, String remoteWorkingDirectory) {
    try {
    Path paths = Paths.get(fileToTransfer);
    String localFile = paths.getFileName().toString();
    String[] command = new String[]{"rsync", "-e /usr/local/bin/ssh -i " + keyFile.getAbsolutePath(),
      fileToTransfer, user + "@" + host + ":" + remoteWorkingDirectory + "/" + localFile};
    if (verbose) {
      System.out.println("CMD: " + Arrays.toString(command));
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

  public static void main(String[] arg) {
    String host = "ec2-23-21-120-250.compute-1.amazonaws.com";
    String file = "GNS.jar";
    String localFile = "/Users/westy/Documents/Code/GNS/dist/" + file;
    String remoteDir = "/home/ec2-user";
    File keyFile = new File("/Users/westy/aws.pem");
    upload("ec2-user", host, keyFile, localFile, remoteDir);
    System.exit(0);
  }
}
