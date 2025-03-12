package edu.uob.dbmodel;

import java.util.*;
import java.util.stream.Collectors;

public class Row {

    LinkedHashMap<Header,String> value = new LinkedHashMap<>();

    public Map<Header, String> getValue() {
        return value;
    }

    public String getColumnValue(Header header) {
        return value.get(header);
    }

    public void setValue(Header header, String value) {
        this.value.put(header, value);
    }

    public void setValue(LinkedHashMap<Header, String> value) {
        this.value = value;
    }

    public List<String> getRowValues() {
        return value.values().stream().toList();
    }

    public List<String> getRowHeaderValues() {
        return value.keySet().stream().map(Header::getName).collect(Collectors.toList());
    }

    public void deleteHeaderValue(String columnName) {
        Header header = value.keySet().stream().filter(head -> head.getName().equals(columnName)).findFirst().get();
        value.remove(header);
    }

    public void addRowValue(Header header, String value) {
        this.value.put(header, value);
    }

    public void updateRow(Header header, String value) {
        this.value.put(header, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Header, String> entry : value.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}
