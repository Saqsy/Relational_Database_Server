package edu.uob.dbmodel;

import edu.uob.exceptions.DatabaseOperationException;

import java.util.*;
import java.util.stream.Collectors;

public class Row {

    LinkedHashMap<Header,String> value = new LinkedHashMap<>();

    public String getColumnValue(Header header) {
        return value.get(header);
    }

    public void setValue(Header header, String value) {
        this.value.put(header, value);
    }

    public List<String> getRowValues() {
        return value.values().stream().toList();
    }

    public List<String> getRowHeaderValues() {
        return value.keySet().stream().map(Header::getName).collect(Collectors.toList());
    }

    public void deleteHeaderValue(String columnName) throws DatabaseOperationException {
        Header header = value.keySet().stream()
                .filter(head -> head.getName().equals(columnName))
                .findFirst().orElseThrow(() -> new DatabaseOperationException(" Table operation failed"));
        value.remove(header);
    }

    public void addRowValue(Header header, String value) {
        this.value.put(header, value);
    }

    public void updateRow(Header header, String value) {
        this.value.put(header, value);
    }

}
