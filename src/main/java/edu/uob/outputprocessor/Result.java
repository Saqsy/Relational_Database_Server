package edu.uob.outputprocessor;

public enum Result {
    SUCCESS("[OK]"),
    FAILURE("[ERROR]");

    public final String value;

    Result(String result) {
        this.value = result;
    }
}
