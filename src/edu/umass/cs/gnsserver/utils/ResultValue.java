
package edu.umass.cs.gnsserver.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;


public class ResultValue extends ArrayList<Object> {

  private static final long serialVersionUID = 1L;


  public ResultValue() {
  }


  public ResultValue(Collection<? extends Object> collection) {
    super(collection);
  }


  public ResultValue(String string) throws JSONException {
    super(JSONUtils.JSONArrayToArrayList(new JSONArray(string)));
  }


  public ResultValueString toResultValueString() {
    ResultValueString result = new ResultValueString();
    for (Object element : this) {
      result.add((String) element);
    }
// Android doesn't like lambdas as of 9-16
//    this.stream().forEach((element) -> {
//      result.add((String) element);
//    });
    return result;
  }


  public Set<String> toStringSet() {
    Set<String> result = new HashSet<>();
    for (Object element : this) {
      result.add((String) element);
    }
// Android doesn't like lambdas as of 9-16
//    this.stream().forEach((element) -> {
//      result.add((String) element);
//    });
    return result;
  }
  
}
