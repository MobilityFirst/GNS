
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gnsserver.main.GNSConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;


public class HostFileLoader {

  private static final Long INVALID_FILE_VERSION = -1L;
  private static Long fileVersion = INVALID_FILE_VERSION;

  private static boolean hostFileHasNodeIds = false;


  public static List<HostSpec> loadHostFile(String hostsFile) throws Exception {
    hostFileHasNodeIds = false;
    List<HostSpec> result = new ArrayList<>();
    BufferedReader br = new BufferedReader(new FileReader(hostsFile));
    boolean readFirstLine = false;
    try {
      while (br.ready()) {
        String line = br.readLine();
        if (line == null || line.equals("") || line.equals(" ")
                || line.startsWith("#")) {
          // do nothing
        } else if (!readFirstLine && isLineTheFileVersion(line)) {
          fileVersion = getTheVersionFromLine(line);
          GNSConfig.getLogger().log(Level.FINE, "Read version line: {0}", fileVersion);
        } else {
          result.add(parseHostline(line));
        }
        readFirstLine = true;
      }
      br.close();
    } catch (IOException e) {
      throw new Exception("Problem reading hosts file: " + e);
    }
    return result;
  }


  private static HostSpec parseHostline(String line) throws IOException {
    String[] tokens = line.split("\\s+");
    if (tokens.length > 1) {
      // hostname ipAddressInDotNotation (FOR LNS ONLY)
      if (tokens.length == 2 && tokens[1].contains(".")) {
        return new HostSpec(tokens[0], tokens[0], tokens[1], null);
        // id hostname port
        // id hostname ipAddressInDotNotation
        // id hostname port ipAddressInDotNotation
      } else {
        String idString = tokens[0];
        String externalIP = null;
        Integer port = null;
        if (tokens.length > 2) {
          if (tokens[2].contains(".")) {
            externalIP = tokens[2];
            port = null;
          } else {
            port = Integer.parseInt(tokens[2]);
          }
        }
        if (tokens.length > 3) {
          if (port == null) {
            throw new IOException("Bad port spec on line " + line + ": " + tokens[2]);
          }
          externalIP = tokens[3];
        }
        hostFileHasNodeIds = true;
        // Parse as an Integer if we can otherwise a String.
//        Object nodeID = -1;
//        try {
//          nodeID = Integer.parseInt(idString);
//        } catch (NumberFormatException e) {
        String nodeID = idString;
        //}
        GNSConfig.getLogger().log(Level.FINE, "Read ID: {0}", nodeID);
//        if (tokens[1].contains(".") && !tokens[1].equals("127.0.0.1")) {
//          throw new IOException("Bad hostname " + tokens[1] + ". Should not be an ip address.");
//        }
        return new HostSpec(nodeID, tokens[1], externalIP, port);
      }
      // hostname
    } else if (tokens.length == 1) {
      if (hostFileHasNodeIds) {
        throw new IOException("Can't mix format with IDs provided and not provided:" + line);
      }
      return new HostSpec(tokens[0], tokens[0], null, null);
    } else {
      throw new IOException("Bad host format:" + line);
    }
  }


  public static Long readVersionLine(String hostsFile) throws IOException {
    String line;
    try (BufferedReader br = new BufferedReader(new FileReader(hostsFile))) {
      line = br.readLine();
    }
    return getTheVersionFromLine(line);
  }


  public static boolean isChangedFileVersion(String hostsFile) throws IOException {
    Long newVersion = readVersionLine(hostsFile);
    GNSConfig.getLogger().log(Level.FINE, "Old version: {0} new version: {1}", 
            new Object[]{fileVersion, newVersion});
    return newVersion != null
            && !Objects.equals(newVersion, INVALID_FILE_VERSION)
            && !Objects.equals(newVersion, fileVersion);
  }

  private static Long getTheVersionFromLine(String line) {
    Long version = null;
    String[] tokens = line.split("\\s+");
    // check to see if it's a host spec on the first line
    if (tokens.length == 1) {
      try {
        version = Long.parseLong(line);
      } catch (NumberFormatException e) {
      }
    }
    return version;
  }

  private static boolean isLineTheFileVersion(String line) {
    return getTheVersionFromLine(line) != null;
  }


  public static Long getFileVersion() {
    return fileVersion;
  }

}
