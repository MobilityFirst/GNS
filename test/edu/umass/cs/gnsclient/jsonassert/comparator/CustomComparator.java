package edu.umass.cs.gnsclient.jsonassert.comparator;

import edu.umass.cs.gnsclient.jsonassert.Customization;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareResult;
import edu.umass.cs.gnsclient.jsonassert.ValueMatcherException;
import org.json.JSONException;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 * @author westy
 */
public class CustomComparator extends DefaultComparator {

    private final Collection<Customization> customizations;

  /**
   *
   * @param mode
   * @param customizations
   */
  public CustomComparator(JSONCompareMode mode,  Customization... customizations) {
        super(mode);
        this.customizations = Arrays.asList(customizations);
    }

  /**
   *
   * @param prefix
   * @param expectedValue
   * @param actualValue
   * @param result
   * @throws JSONException
   */
  @Override
    public void compareValues(String prefix, Object expectedValue, Object actualValue, JSONCompareResult result) throws JSONException {
        Customization customization = getCustomization(prefix);
        if (customization != null) {
            try {
    	        if (!customization.matches(prefix, actualValue, expectedValue, result)) {
                    result.fail(prefix, expectedValue, actualValue);
                }
            }
            catch (ValueMatcherException e) {
                result.fail(prefix, e);
            }
        } else {
            super.compareValues(prefix, expectedValue, actualValue, result);
        }
    }

    private Customization getCustomization(String path) {
        for (Customization c : customizations)
            if (c.appliesToPath(path))
                return c;
        return null;
    }
}
