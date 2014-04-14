package edu.umass.cs.gns.test;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.workloads.Constant;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 *
 * Created by abhigyan on 2/28/14.
 */
public class TraceRequestGenerator {

  /******* BEGIN: during experiments, these methods read workload trace files. Not used outside experiments. ********/

  public static void genRequests(String workloadFile, String lookupTraceFile, String updateTraceFile, double lookupRate,
                                 double updateRate, ScheduledThreadPoolExecutor executorService)
          throws IOException, InterruptedException{

    WorkloadParams workloadParams = new WorkloadParams(workloadFile);

    List<TestRequest> lookupTrace = null;
    List<TestRequest> updateTrace = null;

    if (lookupTraceFile != null) {
      lookupTrace = readTrace(lookupTraceFile, TestRequest.LOOKUP);
    }

    if (updateTraceFile != null) {
      updateTrace = readTrace(updateTraceFile, TestRequest.UPDATE);
    }

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Scheduling all lookups.");
    }
    new RequestGenerator().generateRequests(workloadParams, lookupTrace, new Constant(lookupRate), executorService);

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Scheduling all updates.");
    }
    new RequestGenerator().generateRequests(workloadParams, updateTrace, new Constant(updateRate), executorService);
  }

  private static List<TestRequest> readTrace(String filename, int defaultRequestType) throws IOException {
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
      if (tokens.length == 1){ // by default
        trace.add(new TestRequest(tokens[0], defaultRequestType));
      } else if (Integer.parseInt(tokens[1]) == TestRequest.GROUP_CHANGE) {
        trace.add(new TestGroupChangeRequest(tokens[0], line));
      } else if (tokens.length == 2) {
        trace.add(new TestRequest(tokens[0], new Integer(tokens[1])));
      }
    }
    br.close();
    return trace;
  }

  /******* END: during experiments, these methods read workload trace files. Not used outside experiments. ********/


}
