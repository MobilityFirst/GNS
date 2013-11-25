package edu.umass.cs.gns.test;

/**
 * Creates different failure scenarios that are tested in the code
 * User: abhigyan
 * Date: 11/14/13
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public enum FailureScenario {

  applyNewActivesProposed("applyNewActivesProposed"),
  handleOldActivesStopConfirmMessage("handleOldActivesStopConfirmMessage"),
  handleNewActiveStartConfirmMessage("handleNewActiveStartConfirmMessage"),
  applyActiveNameServersRunning("applyActiveNameServersRunning");

  String methodName;

  FailureScenario(String methodName) {
    this.methodName = methodName;
  }


  public boolean equals(FailureScenario failureScenario) {
    return methodName.equals(failureScenario.toString());
  }

}
