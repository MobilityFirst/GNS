
package edu.umass.cs.aws.networktools;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class SFTPUpload {

  private static boolean verbose = false;


  public static void uploadFile(String user, String host, File keyFile, String fileToTransfer, String sftpWorkingDirectory) {
    if (verbose) {
      System.out.println("Upload file from " + fileToTransfer + " to " + host + "@" + user + " " + sftpWorkingDirectory);
    }
    try {
      ChannelSftp channelSftp = authenticateSftp(user, host, keyFile);
      File f = new File(fileToTransfer);
      channelSftp.put(new FileInputStream(f), f.getName());
    } catch (JSchException | SftpException | FileNotFoundException e) {
      System.out.println("Exception while uploading file:" + e);
    }
  }


  public static boolean localFileNewer(String user, String host, File keyFile, String fileToTransfer, String sftpWorkingDirectory) {
    if (verbose) {
      System.out.println("Local File Newer Check " + fileToTransfer + " to " + host + "@" + user + " " + sftpWorkingDirectory);
    }
    try {
      ChannelSftp channelSftp = authenticateSftp(user, host, keyFile);
      Path paths = Paths.get(fileToTransfer);
      String localDir = paths.getParent().toString();
      channelSftp.cd(sftpWorkingDirectory);
      channelSftp.lcd(localDir);
      SftpATTRS remoteAttributes = channelSftp.stat(paths.getFileName().toString());
      long localTime = new File(fileToTransfer).lastModified();
      long remoteTime = remoteAttributes.getMTime() * 1000L;
      if (verbose) {
        System.out.println("L: " + localDir + " R: " + sftpWorkingDirectory + "\n"
                + "Local time = " + localTime + " Remote time = " + remoteTime);
      }
      if (verbose) {
        System.out.println("Result " + (localTime > remoteTime));
      }
      return localTime > remoteTime;
    } catch (JSchException | SftpException e) {
      System.out.println("Exception while checking for file newer:" + e);
      return false;
    }
  }

  private static ChannelSftp authenticateSftp(String user, String host, File keyFile) throws JSchException {
    Session session;
    Channel channel;
    JSch jsch = new JSch();
    session = SSHClient.authenticateWithKey(jsch, user, host, keyFile);
    session.connect();
    channel = session.openChannel("sftp");
    channel.connect();
    return (ChannelSftp) channel;
  }
  

  public static boolean isVerbose() {
    return verbose;
  }


  public static void setVerbose(boolean verbose) {
    SFTPUpload.verbose = verbose;
  }


  public static void main(String[] arg) {
    String host = "ec2-23-21-120-250.compute-1.amazonaws.com";
    String file = "installScript.sh";
    String localFile = "/Users/westy/" + file;
    String remoteDir = "/home/ec2-user";
    File keyFile = new File("/Users/westy/aws.pem");
    localFileNewer("ec2-user", host, keyFile, localFile, remoteDir);
    System.exit(0);
  }
}
