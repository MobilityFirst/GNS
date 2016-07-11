package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.IOException;

import javax.script.ScriptEngine;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
 
public class MyClassFilterTest {
 
  static class MyCF implements ClassFilter {
    @Override
    public boolean exposeToScripts(String s) {
      if (s.compareTo("java.io.File") == 0) return false;
      return true;
    }
  }
 
  public void testClassFilter() {
 
    final String script =
      "print(java.lang.System.getProperty(\"java.home\"));" +
      "print(\"Create file variable\");" +
      "var File = Java.type(\"java.io.File\");";
 
    NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
 
    ScriptEngine engine = factory.getScriptEngine(
      new MyClassFilterTest.MyCF());
    try {
      engine.eval(script);
    } catch (Exception e) {
      System.out.println("Exception caught: " + e.toString());
    }
  }
 
  public static void main(String[] args) throws IOException {
    MyClassFilterTest myApp = new MyClassFilterTest();
    myApp.testClassFilter();
  }
}