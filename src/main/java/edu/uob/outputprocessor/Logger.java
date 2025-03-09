package edu.uob.outputprocessor;

public class Logger {

    static StringBuilder builder = new StringBuilder();

    public static String getResult() {
        return builder.toString();
    }

    public static void logResult(String result) {
        builder.append(result);
    }

    public static void flush() {
        builder = new StringBuilder();
    }

}
