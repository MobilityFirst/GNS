/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 *
 * @author westy
 */
public class RetrievableDigest {

  private final MessageDigest hashFunction;
  // Stores the info for the digest in a FIFO queue. Used to remove old entries.
  private final Queue<DigestInfo> timeQueue;
  // Stores the info the digest in a map. Ues to retrieve the original string from the digest.
  private final Map<String, DigestInfo> digestMap;
  // How long (in milleseconds) to keep entries in our table before we discard them
  private final long timeToLive;

  /**
   *
   * @param timeToLive - how long (in milleseconds) to keep entries in our table before we discard them
   * @throws NoSuchAlgorithmException
   */
  public RetrievableDigest(long timeToLive) throws NoSuchAlgorithmException {
    hashFunction = MessageDigest.getInstance("SHA-256");
    this.timeQueue = new ConcurrentLinkedQueue();
    this.digestMap = new ConcurrentHashMap();
    this.timeToLive = timeToLive;
  }

  /**
   * Creates a digest of a string that can be later retrieved from the digest.
   *
   * @param request
   * @return
   */
  public byte[] createDigest(String request) {
    DigestInfo digestInfo = new DigestInfo(request);
    this.timeQueue.add(digestInfo);
    this.digestMap.put(digestInfo.digestString, digestInfo);
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("********** Created " + request);
    }
    return digestInfo.digest;
  }

  /**
   * Retrieves the original string that was used to create the digest.
   * Will return null if no corresponding original string value exists for the
   * given digest value.
   *
   * @param digest
   * @return
   */
  public String retrieveOriginalString(byte[] digest) {
    String string = new String(digest);
    DigestInfo digestInfo = digestMap.get(string);
    if (digestInfo != null) {
      digestMap.remove(digestInfo.digestString);
      removeOldInfo();
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("********** Retrieved " + string);
      }
      return digestInfo.request;
    } else {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("********** Original string not found for " + string);
      }
      return null;
    }
  }

  /**
   * Pulls old entries out and removes them if the are older than the
   * <code>timeToLive</code> given to the constructor.
   */
  private void removeOldInfo() {
    boolean done = false;
    do {
      DigestInfo oldestInfo;
      long currentTime = System.currentTimeMillis();
      if ((oldestInfo = timeQueue.peek()) != null) {
        if (currentTime - oldestInfo.timestamp > timeToLive) {

          timeQueue.remove();
          digestMap.remove(oldestInfo.digestString);
          GNS.getLogger().log(Level.INFO, "Removed {0} digest map remaining:{1} time queue remaining:{2}",
                  new Object[]{oldestInfo.request, digestMap.size(), timeQueue.size()});
        } else {
          // oldest entry in timeQueue is less than 5 minutes old
          done = true;
        }
      } else {
        // timeQueue is empty
        done = true;
      }
    } while (!done);
  }

  private class DigestInfo {

    private String request;
    private long timestamp;
    private byte[] digest;
    private String digestString;

    public DigestInfo(String request) {
      this.request = request;
      this.timestamp = System.currentTimeMillis();
      this.digest = hashFunction.digest(request.getBytes());
      this.digestString = new String(digest);
    }
  }

  // Test code
  private static long fiveMinutes = TimeUnit.MINUTES.toMillis(5);
  private static long oneMinute = TimeUnit.MINUTES.toMillis(1);
  private static long fiveSeconds = TimeUnit.SECONDS.toMillis(5);

  public static void main(String[] args) throws NoSuchAlgorithmException {
    Random random = new Random();
    RetrievableDigest requestDigest = new RetrievableDigest(fiveSeconds);
    do {
      String originalRequest = "Request" + Util.randomString(6);
      GNS.getLogger().info("Created " + originalRequest);
      byte[] digest = requestDigest.createDigest(originalRequest);
      if (random.nextBoolean()) {
        String request = requestDigest.retrieveOriginalString(digest);
        GNS.getLogger().info("Retrieved " + request);
      }
      ThreadUtils.sleep(1000);
    } while (true);
  }

}
