package edu.uob.dbmodel;

import edu.uob.exceptions.DatabaseOperationException;
import edu.uob.storageprocessor.StorageEngine;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Table {

    List<Header> headers = new ArrayList<>();
    List<Row> rows = new ArrayList<>();

    public List<Header> getHeaders() {
        return headers;
    }

    public void addHeader(Header header) {
        headers.add(header);
    }

    public void deleteHeader(String headerName) throws DatabaseOperationException {
        Header header = headers.stream().filter(head -> head.getName().equals(headerName))
                .findFirst()
                .orElseThrow(() -> new DatabaseOperationException(" Table operation failed"));
        if (header != null) {
            headers.remove(header);
        }
    }

    public void deleteRow(int index) {
        Row row = rows.get(index);
        rows.remove(row);
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public void addRow(Row row) {
        rows.add(row);
    }

    public void writeTableToFile(File file) throws DatabaseOperationException {
        StorageEngine storageEngine = new StorageEngine();
        storageEngine.getFileWriter(file);
        storageEngine.writeData(this.toString());
        storageEngine.flushReader();
    }

    public List<String> getHeaderValues() {
        List<String> values = new ArrayList<>();
        headers.forEach(header -> values.add(header.getName()));
        return values;
    }

    public void readTableData(File file) throws DatabaseOperationException {
        StorageEngine storageEngine = new StorageEngine();
        storageEngine.getFileReader(file);
        // Read entire file content.
        String headerLine;
        headerLine = storageEngine.readLine();
        if (headerLine == null) {
            // If file is empty, create a header with auto-generated "id" column.
            headers.add(new Header("id"));
        }
        // Add headers to list
        Arrays.stream(headerLine.split("\t")).forEach(header -> {
            headers.add(new Header(header));
        });
        // Add rows
        String line;
        while ((line = storageEngine.readLine()) != null) {
            Row row = new Row();
            List<String> values = new ArrayList<>(List.of(line.split("\t")));
            if (headers.size() > values.size()) {
                values.add("");
            }
            for (int i = 0; i < headers.size(); i++) {
                row.setValue(headers.get(i), values.get(i));
            }
            rows.add(row);
        }
        storageEngine.flushWriter();
    }

    public void deleteColumn(String columnName) throws DatabaseOperationException {
        deleteHeader(columnName);
        for (Row row : rows) {
            row.deleteHeaderValue(columnName);
        }
    }

    public void addColumn(String columnName) {
        Header header = new Header(columnName);
        addHeader(header);
        rows.stream().forEach(row -> {
            row.addRowValue(header, "");
        });
    }

    public boolean containsColumn(String columnName) {
        return headers.stream().filter(header -> header.getName().equals(columnName)).findFirst().isPresent();
    }

    public Row getRow(int i) {
        return rows.get(i);
    }

    public Header getHeader(int i) {
        return headers.get(i);
    }

    public void updateRow(int i, String column, String value) {
        Header header = getColumn(column);
        if (header != null) {
            rows.get(i).updateRow(header, value);
        }
    }

    public Header getColumn(String column) {
        return headers.stream().filter(header -> header.getName().equals(column)).findFirst().orElse(null);
    }

    public void addNewRow() {
        Row row = new Row();
        headers.forEach(header -> row.addRowValue(header, ""));
        rows.add(row);
    }

    public String getColumnValueForRow(int i, String column) {
        return rows.get(i).getColumnValue(getColumn(column));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (headers != null && !headers.isEmpty()) {
            sb.append(String.join("\t", getHeaderValues()));
        }
        if (rows != null && !rows.isEmpty()) {
            rows.forEach(row -> {
                sb.append("\n");
                sb.append(String.join("\t", row.getRowValues()));
            });
        }

        return sb.toString();
    }
}
