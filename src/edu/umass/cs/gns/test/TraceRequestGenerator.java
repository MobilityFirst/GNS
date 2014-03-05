package edu.umass.cs.gns.test;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.util.TestRequest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by abhigyan on 2/28/14.
 */
public class TraceRequestGenerator {


  /*******BEGIN: during experiments, these methods read workload trace files. Not used outside experiments. ********/


  public static void generateLookupsUpdates(String lookupTraceFile, String updateTraceFile, double lookupRate,
                                            double updateRate, ScheduledThreadPoolExecutor executorService)
          throws IOException, InterruptedException{

    List<TestRequest> lookupTrace = null;
    List<TestRequest> updateTrace = null;

    if (lookupTraceFile != null) {
      lookupTrace = readLookupTrace(lookupTraceFile);
    }

    if (updateTraceFile != null) {
      updateTrace = readUpdateTrace(updateTraceFile);
    }

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Scheduling all lookups.");
    }
    new RequestGenerator().generateRequests(lookupTrace, lookupRate, executorService);

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Scheduling all updates via intercessor.");
    }
    new RequestGenerator().generateRequests(updateTrace, updateRate, executorService);
  }


  /**
   **
   * Reads a file containing the workload for this local name server. The method returns a Set containing names that can
   * be queried by this local name server.
   *
   * @param filename Workload file
   * @return Set containing names that can be queried by this local name server.
   * @throws IOException
   */
  private static Set<String> readWorkloadFile(String filename) throws IOException {
    Set<String> workloadSet = new HashSet<String>();

    BufferedReader br = new BufferedReader(new FileReader(filename));
    while (br.ready()) {
      final String name = br.readLine().trim();
      if (name != null) {
        workloadSet.add(name);
      }
    }
    return workloadSet;
  }

  private static List<TestRequest> readLookupTrace(String filename) throws IOException {
    List<TestRequest> trace = new ArrayList<TestRequest>();
    BufferedReader br = new BufferedReader(new FileReader(filename));
    while (br.ready()) {
      String line = br.readLine(); //.trim();
      if (line == null) {
        continue;
      }
      line = line.trim();
      if (line.length() == 0) continue;
      // name type (add/remove/update)
      String[] tokens = line.split("\\s+");
      if (tokens.length == 2) {
        trace.add(new TestRequest(tokens[0], new Integer(tokens[1])));
        continue;
      } else {
        trace.add(new TestRequest(tokens[0], TestRequest.LOOKUP));
      }

    }
    br.close();
    return trace;
  }

  private static List<TestRequest> readUpdateTrace(String filename) throws IOException {
    List<TestRequest> trace = new ArrayList<TestRequest>();
    BufferedReader br = new BufferedReader(new FileReader(filename));
    while (br.ready()) {
      String line = br.readLine(); //.trim();
      if (line == null) {
        continue;
      }
      line = line.trim();
      if (line.length() == 0) continue;
      // name type (add/remove/update)
      String[] tokens = line.split("\\s+");
      if (tokens.length == 2) {
        trace.add(new TestRequest(tokens[0], new Integer(tokens[1])));
        continue;
      } else {
        trace.add(new TestRequest(tokens[0], TestRequest.UPDATE));
      }

    }
    br.close();
    return trace;
  }

//  /**
//   **
//   * Checks whether <i>name</i> is in the workload set for this local name server.
//   *
//   * @param name Host/device/domain name
//   * @return <i>true</i> if the workload contains <i>name</i>, <i>false</i> otherwise
//   *
//   */
//  public static boolean workloadContainsName(Set<SString name) {
//    return workloadSet.contains(name);
//  }

  /*******END: during experiments, these methods read workload trace files. Not used outside experiments. ********/

}
