package edu.uob.dbengine;

import edu.uob.dbmodel.Header;
import edu.uob.dbmodel.Row;
import edu.uob.dbmodel.Table;
import edu.uob.outputprocessor.Logger;
import edu.uob.outputprocessor.Result;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseOperationHandler {

    private static final String DATABASE_DIR = Paths.get("databases").toAbsolutePath().toString();

    private static class CurrentDatabase {
        static String name = null;
        static String path = null;
    }

    private boolean isDatabaseInactive() {
        if (CurrentDatabase.name == null) {
            System.out.println("No database selected");
            return true;
        }
        return false;
    }

    private File getTableFile(String tableName) {
        //TODO handle exception
        File tableFile = new File(CurrentDatabase.path, tableName + ".tab");
        if (tableFile.exists()) {
            return tableFile;
        }
        return null;
    }

    public Result useDatabase(String dbName) {
        File dbDir = new File(DATABASE_DIR, dbName);
        if (dbDir.exists() && dbDir.isDirectory()) {
            CurrentDatabase.name = dbName;
            CurrentDatabase.path = dbDir.getAbsolutePath();
            return Result.SUCCESS;
        } else {
            return Result.FAILURE;
        }
    }

    public Result createDatabase(String dbName) {
        File dbDir = new File(DATABASE_DIR, dbName);
        if (!dbDir.exists()) {
            dbDir.mkdir();
            CurrentDatabase.name = dbName;
            CurrentDatabase.path = dbDir.getAbsolutePath();
            return Result.SUCCESS;
        } else {
            return Result.FAILURE;
        }
    }

    public Result createTable(String tableName, List<String> attributes) {
        if (CurrentDatabase.name == null) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = new File(CurrentDatabase.path, tableName + ".tab");
        try {
            if (tableFile.createNewFile()) {
                if (attributes != null && !attributes.isEmpty()) {
                    // Write attribute names as header in the table file
                    if (!attributes.contains("id")) {
                        attributes.add(0, "id");
                    }
                    Table newTable = new Table();
                    attributes.forEach(attr -> newTable.addHeader(new Header(attr)));
                    newTable.writeTableToFile(tableFile);
                }
                System.out.println("Table created: " + tableName);
                return Result.SUCCESS;
            } else {
                System.out.println("Table already exists: " + tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    //TODO check insert
    public Result insertIntoTable(String tableName, List<String> values) {
        if (CurrentDatabase.name == null) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = new File(CurrentDatabase.path, tableName + ".tab");

        Table table = new Table();
        table.readTableData(tableFile);

        Row row = new Row();
        int nextId = !table.getRows().isEmpty() ? table.getRows().size()+1 : 1;
        row.setValue(table.getHeaders().get(0), String.valueOf(nextId));
        for (int i = 1; i < table.getHeaders().size(); i++) {
            row.setValue(table.getHeaders().get(i), values.get(i-1));
        }
        table.addRow(row);
        table.writeTableToFile(tableFile);

        return Result.SUCCESS;
    }

    //TODO select with multiple conditions with AND pending
    public Result selectFromTable(String tableName, List<String> attributes, String condition) {
        //TODO write current db check and file read logic into different method
        if (isDatabaseInactive()) {
            return Result.FAILURE;
        }

        File tableFile = getTableFile(tableName);
        Table tableResult = new Table();
        tableResult.readTableData(tableFile);
        Table table = new Table();
        table.readTableData(tableFile);

        // Determine which columns to output
        if (!attributes.get(0).equals("*")) {
            List<String> columns = new ArrayList<>();
            columns.addAll(tableResult.getHeaderValues());
            for (String attr : attributes) {
                columns.remove(attr);
            }
            columns.forEach(tableResult::deleteHeader);
            columns.forEach(tableResult::deleteColumn);
        }

        // Process each row.
        List<Row> newRows = new ArrayList<>();

        for (int i = 0; i < table.getRows().size(); i++) {

            if (evaluateMultipleConditions(condition,
                    table.getHeaderValues().stream().toList().toArray(new String[0]),
                    table.getRow(i).getRowValues().stream().toList().toArray(new String[0]))) {
                newRows.add(tableResult.getRows().get(i));
            }
        }

        tableResult.setRows(newRows);
        Logger.logResult("\n");
        Logger.logResult(tableResult.toString());

        return Result.SUCCESS;
    }

    private boolean evaluateMultipleConditions(String condition, String[] header, String[] row) {
        if (condition == null || condition.trim().isEmpty()) {
            return true;
        }
        // If the condition does not contain any parentheses, treat it as a single condition.
        if (!condition.contains("(") && !condition.contains(")")) {
            return evaluateSingleCondition(condition, header, row);
        }

        // Otherwise, extract conditions enclosed in parentheses.
        Pattern p = Pattern.compile("\\(([^)]+)\\)");
        Matcher m = p.matcher(condition);
        List<String> conditions = new ArrayList<>();
        while (m.find()) {
            conditions.add(m.group(1).trim());
        }

        // Determine the boolean operator: if "OR" exists, use OR; otherwise, use AND.
        String upperCond = condition.toUpperCase();
        boolean useOr = upperCond.contains(" OR ");

        // If no conditions were extracted (e.g. malformed input), evaluate the entire condition.
        if (conditions.isEmpty()) {
            return evaluateSingleCondition(condition, header, row);
        }

        if (useOr) {
            for (String cond : conditions) {
                if (evaluateSingleCondition(cond, header, row)) {
                    return true;
                }
            }
            return false;
        } else {
            for (String cond : conditions) {
                if (!evaluateSingleCondition(cond, header, row)) {
                    return false;
                }
            }
            return true;
        }
    }

    private boolean evaluateSingleCondition(String cond, String[] header, String[] row) {
        // Expected format: [AttributeName] <Comparator> [Value]
        Pattern conditionPattern = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = conditionPattern.matcher(cond);
        if (matcher.matches()) {
            String condAttr = matcher.group(1);
            String condOperator = matcher.group(2);
            String condValue = matcher.group(3).trim();
            // Remove surrounding quotes if present.
            condValue = condValue.replaceAll("^'|'$", "");
            // Find the attribute index.
            int colIndex = -1;
            for (int i = 0; i < header.length; i++) {
                if (header[i].equalsIgnoreCase(condAttr)) {
                    colIndex = i;
                    break;
                }
            }
            if (colIndex == -1) {
                System.out.println("Condition attribute not found: " + condAttr);
                return false;
            }
            String cellValue = row.length > colIndex ? row[colIndex] : "";
            return evaluateCondition(cellValue, condOperator, condValue);
        } else {
            System.out.println("Invalid condition format: " + cond);
            return false;
        }
    }

    private boolean evaluateCondition(String cellValue, String operator, String condValue) {
        try {
            double dCell = Double.parseDouble(cellValue);
            double dCond = Double.parseDouble(condValue);
            switch (operator) {
                case "==": return dCell == dCond;
                case "!=": return dCell != dCond;
                case ">":  return dCell > dCond;
                case "<":  return dCell < dCond;
                case ">=": return dCell >= dCond;
                case "<=": return dCell <= dCond;
                default: return false;
            }
        } catch (NumberFormatException e) {
            // Non-numeric comparison.
            if (operator.equalsIgnoreCase("LIKE")) {
                // Simple substring match for LIKE.
                return cellValue.contains(condValue);
            } else if (operator.equals("==")) {
                return cellValue.equals(condValue);
            } else if (operator.equals("!=")) {
                return !cellValue.equals(condValue);
            } else {
                // For other operators, lexicographical comparison.
                int cmp = cellValue.compareTo(condValue);
                switch (operator) {
                    case ">": return cmp > 0;
                    case "<": return cmp < 0;
                    case ">=": return cmp >= 0;
                    case "<=": return cmp <= 0;
                    default: return false;
                }
            }
        }
    }

    public Result dropDatabase(String dbName) {
        File dbDir = new File(DATABASE_DIR, dbName);
        if (dbDir.exists()) {
            deleteDirectory(dbDir);
            return Result.SUCCESS;
        } else {
            return Result.FAILURE;
        }
    }

    public Result dropTable(String tableName) {
        if (isDatabaseInactive()) {
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);
        if (tableFile.exists() && tableFile.delete()) {
            return Result.SUCCESS;
        }
        return Result.FAILURE;
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    public Result deleteFromTable(String tableName, String condition) {

        if (isDatabaseInactive()) {
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);

        Table table = new Table();
        table.readTableData(tableFile);

        // TODO extract below into helper func
        // Parse condition, if provided.
        String condOperator = null;
        String condValue = null;
        String condAttr = null;
        if (condition != null && !condition.isEmpty()) {
            // Expecting format: [AttributeName] <Comparator> [Value]
            Pattern conditionPattern = Pattern.compile(
                    "([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = conditionPattern.matcher(condition);
            if (matcher.matches()) {
                condAttr = matcher.group(1);
                condOperator = matcher.group(2);
                condValue = matcher.group(3).trim();
                // Remove surrounding quotes if present.
                condValue = condValue.replaceAll("^'|'$", "");
                // Find condition attribute index.
                if (!table.containsColumn(condAttr)) {
                    System.out.println("Condition attribute not found: " + condAttr);
                    return Result.FAILURE;
                }
            } else {
                System.out.println("Unsupported condition format: " + condition);
                return Result.FAILURE;
            }
        }

        for (int i = 0; i < table.getRows().size(); i++) {
            String cellValue = table.getRows().get(i).getColumnValue(table.getColumn(condAttr));
            if (evaluateCondition(cellValue, condOperator, condValue)) {
                table.deleteRow(i);
                i--;
            }
        }

        table.writeTableToFile(tableFile);

        return Result.SUCCESS;
    }

    public Result alterTable(String tableName, String alterationType, String attributeName) {
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);

        Table table = new Table();
        table.readTableData(tableFile);

        if (alterationType.equalsIgnoreCase("ADD")) {
            table.addColumn(attributeName);
        } else if (alterationType.equalsIgnoreCase("DROP")) {
            table.deleteColumn(attributeName);
        }
        table.writeTableToFile(tableFile);
        return Result.SUCCESS;
    }

    public Result updateTable(String tableName, Map<String, String> nameValuePairs, String condition) {
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);

        Table table = new Table();
        table.readTableData(tableFile);

        // Parse condition, if provided.
        String condOperator = null;
        String condValue = null;
        String condAttr = null;
        if (condition != null && !condition.isEmpty()) {
            // Expecting format: [AttributeName] <Comparator> [Value]
            Pattern conditionPattern = Pattern.compile(
                    "([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = conditionPattern.matcher(condition);
            if (matcher.matches()) {
                condAttr = matcher.group(1);
                condOperator = matcher.group(2);
                condValue = matcher.group(3).trim();
                // Remove surrounding quotes if present.
                condValue = condValue.replaceAll("^'|'$", "");
                // Find condition attribute index.
                if (!table.containsColumn(condAttr)) {
                    System.out.println("Condition attribute not found: " + condAttr);
                    return Result.FAILURE;
                }
            } else {
                System.out.println("Unsupported condition format: " + condition);
                return Result.FAILURE;
            }
        }

        for (int i = 0; i < table.getRows().size(); i++) {
            String cellValue = table.getRows().get(i).getColumnValue(table.getColumn(condAttr));
            if (evaluateCondition(cellValue, condOperator, condValue)) {
                for (Map.Entry<String, String> entry : nameValuePairs.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    //TODO proper error handling
                    if (!table.containsColumn(key)) {
                        return Result.FAILURE;
                    }
                    table.updateRow(i,key,value);
                }
            }
        }

        table.writeTableToFile(tableFile);

        return Result.SUCCESS;
    }

    public Result joinTables(String tableName1, String tableName2, String attributeName1, String attributeName2) {
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }

        File tableFile1 = getTableFile(tableName1);
        File tableFile2 = getTableFile(tableName2);

        Table table1 = new Table();
        table1.readTableData(tableFile1);
        Table table2 = new Table();
        table2.readTableData(tableFile2);
        Table resultTable = new Table();

        // Add id column
        resultTable.addColumn("id");

        // Add table 1 headers
        for (int i = 1; i < table1.getHeaders().size(); i++) {
            if (!table1.getHeader(i).getName().equals(attributeName1)) {
                resultTable.addColumn(tableName1 + "." + table1.getHeader(i).getName());
            }
        }

        // Add table 2 headers
        for (int i = 1; i < table2.getHeaders().size(); i++) {
            if (!table2.getHeader(i).getName().equals(attributeName2)) {
                resultTable.addColumn(tableName2 + "." + table2.getHeader(i).getName());
            }
        }

        // Perform Join
        int newId = 1;
        int row = 0;
        for (int i = 0; i < table1.getRows().size(); i++) {
            String TableOneCellValue = table1.getRows().get(i).getColumnValue(table1.getColumn(attributeName1));
            for (int j = 0; j < table2.getRows().size(); j++) {
                String TableTwoCellValue = table2.getRows().get(j).getColumnValue(table2.getColumn(attributeName2));
                if (TableOneCellValue.equals(TableTwoCellValue)) {
                    resultTable.addNewRow();
                    resultTable.updateRow(row,"id", String.valueOf(newId++));
                        Row row1 = table1.getRow(i);
                        if (attributeName1.equals("id")) {
                            row1.deleteHeaderValue(attributeName1);
                        } else {
                            row1.deleteHeaderValue("id");
                            row1.deleteHeaderValue(attributeName1);
                        }
                        for (int l = 0; l < row1.getRowValues().size(); l++) {
                            resultTable.updateRow(row,
                                    tableName1 + "." + row1.getRowHeaderValues().get(l),
                                    String.valueOf(row1.getRowValues().get(l)));
                        }

                    Row row2 = table2.getRow(j);
                    if (attributeName2.equals("id")) {
                        row2.deleteHeaderValue(attributeName2);
                    } else {
                        row2.deleteHeaderValue("id");
                        row2.deleteHeaderValue(attributeName2);
                    }
                    for (int l = 0; l < row2.getRowValues().size(); l++) {
                        resultTable.updateRow(row,
                                tableName2 + "." + row2.getRowHeaderValues().get(l),
                                String.valueOf(row2.getRowValues().get(l)));
                    }
                    row++;
                }
            }
        }
        Logger.logResult("\n");
        Logger.logResult(resultTable.toString());
        return Result.SUCCESS;

    }


}


