package com.example.regextokenreplacement;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexApp {

    public static void main(String[] args) {
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        String regex = "(\\w+)=(.*?)(?:,|})";
        String line = "[{account=12312421, class=45326, department=}]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        output.append("[{");
        while(matcher.find()) {
            output.append('"' + matcher.group(1) + '"' +":"+ '"' + matcher.group(2) + '"' + ",");


        }
        output.setLength(output.length()-1);
        output.append("}]");
        System.out.println(output);

    }
}