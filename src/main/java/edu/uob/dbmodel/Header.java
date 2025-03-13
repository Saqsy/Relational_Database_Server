package edu.uob.dbmodel;

import java.util.Objects;

public class Header {
    private final String name;

    public Header(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Header header)) return false;
        return Objects.equals(name, header.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
