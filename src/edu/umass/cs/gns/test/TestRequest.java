package edu.umass.cs.gns.test;

/**
 * A request we send from local name server to name server during experiments, and in testing.
 */
public class TestRequest {
  // request type
  public static final int LOOKUP = 1;
  public static final int UPDATE = 2;
  public static final int ADD = 3;
  public static final int REMOVE = 4;
  public static final int GROUP_CHANGE = 5;

  public static final int DELAY = 6; // this is not a request. it introduces delay between the preceding and the next
                                      // request. the name field for DELAY entry is an integer that specifies the delay.

  public static final int RATE = 7;  // sends subsequent requests at given rate/sec. rate can be specified multiple
                                     // times during a trace to change the rate of later requests

  public final int type;
  public final String name;

  public TestRequest(String name, int type) {
    this.name = name;
    if (type == LOOKUP || type == UPDATE || type == ADD || type == REMOVE || type == DELAY
            || type == GROUP_CHANGE || type == RATE)
      this.type = type;
    else
      throw new IllegalArgumentException("Request type not found: " + type);
  }

  public String toString() {
    return name + ":" + type;
  }

  public static TestRequest parseLine(String line) {
    if (line == null) {
      return null;
    }
    line = line.trim();
    if (line.length() == 0) return null;
    // name type (add/remove/update)
    String[] tokens = line.split("\\s+");
    if (Integer.parseInt(tokens[1]) == TestRequest.GROUP_CHANGE) {
      return new TestGroupChangeRequest(tokens[0], line);
    } else if (tokens.length == 2) {
      return new TestRequest(tokens[0], new Integer(tokens[1]));
    }
    return null;
  }


}
