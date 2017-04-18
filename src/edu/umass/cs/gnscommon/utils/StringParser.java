package edu.umass.cs.gnscommon.utils;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tramteja on 4/17/17.
 */
public class StringParser {
    private static final Pattern regexPattern = Pattern.compile("([^\"]\\S*|\".+?\")\\s*");
    private Matcher m;
    private List<String> list;
    private int index = 0;

    public StringParser(String input){
        m = regexPattern.matcher(input);
        list = new ArrayList<>();
        while (m.find())
            list.add(m.group(1).replace("\"", "").trim());
    }

    /**
     * Utility function to return number of tokens in given input string
     *
     * @return number of entries in the list
     */
    public int countTokens(){
        return list.size();
    }

    /**
     * Utility function to return next token in given input string
     *
     * @return string next token in the stream
     */
    public String nextToken(){
        if(index >= list.size())
            return "";
        int current_index = index;
        index += 1;
        return list.get(current_index);
    }

    /**
     * Utility function to check if any tokens are present in the string
     *
     * @return
     */

    public boolean hasMoreTokens(){
        if (index < list.size())
            return true;
        else
            return false;
    }

}
