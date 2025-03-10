package edu.uob.dbengine;

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
                    try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile))) {
                        pw.println(String.join("\t", attributes));
                    }
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
        if (!tableFile.exists()) {
            System.out.println("Table does not exist: " + tableName);
            return Result.FAILURE;
        }
        try {
            // Read entire file content.
            List<String> lines = new ArrayList<>();
            String headerLine;
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
                headerLine = br.readLine();
                if (headerLine == null) {
                    // If file is empty, create a header with auto-generated "id" column.
                    headerLine = "id";
                }
                lines.add(headerLine);
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }

            // Check if header already contains an 'id' column as the first column.
            String[] header = headerLine.split("\t");
            boolean hasId = (header.length > 0 && header[0].equals("id"));

            // If not, update the header and existing rows to include the auto-generated id column.
            if (!hasId) {
                headerLine = "id\t" + headerLine;
                lines.set(0, headerLine);
                // Prepend an empty id field for each existing data row.
                for (int i = 1; i < lines.size(); i++) {
                    lines.set(i, "\t" + lines.get(i));
                }
            }

            // Next id is computed as the number of data rows + 1.
            int nextId = lines.size();  // header is at index 0

            // Create new row with auto-generated id as the first column.
            String newRow = nextId + "\t" + String.join("\t", values);
            lines.add(newRow);

            // Write all lines back to the table file.
            try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile))) {
                for (String l : lines) {
                    pw.println(l);
                }
            }
            System.out.println("Inserted into table " + tableName + " with id " + nextId);
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    //TODO select with multiple conditions with AND pending
    public Result selectFromTable(String tableName, List<String> attributes, String condition) {
        //TODO write current db check and file read logic into different method
        if (isDatabaseInactive()) {
            return Result.FAILURE;
        }

        File tableFile = getTableFile(tableName);
        if (tableFile == null) {
            return Result.FAILURE;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                System.out.println("Table is empty.");
                return Result.FAILURE;
            }
            String[] columns = headerLine.split("\t");

            // Determine which column indices to output
            List<Integer> outputIndices = new ArrayList<>();
            if (attributes.size() == 1 && attributes.get(0).equals("*")) {
                for (int i = 0; i < columns.length; i++) {
                    outputIndices.add(i);
                }
            } else {
                for (String attr : attributes) {
                    boolean found = false;
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i].equalsIgnoreCase(attr)) {
                            outputIndices.add(i);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        System.out.println("Attribute not found: " + attr);
                        return Result.FAILURE;
                    }
                }
            }

            // Prepare condition filtering, if a condition is provided.
            int conditionColumn = -1;
            String condOperator = null;
            String condValue = null;
            if (condition != null && !condition.isEmpty()) {
                // Expected format: attribute comparator value
                Pattern conditionPattern = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE);
                Matcher matcher = conditionPattern.matcher(condition);
                if (matcher.matches()) {
                    String condAttr = matcher.group(1);
                    condOperator = matcher.group(2);
                    condValue = matcher.group(3).trim();
                    // Remove quotes from condValue if present
                    condValue = condValue.replaceAll("^'|'$", "");
                    // Find index of condition attribute in header
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i].equalsIgnoreCase(condAttr)) {
                            conditionColumn = i;
                            break;
                        }
                    }
                    if (conditionColumn == -1) {
                        System.out.println("Condition attribute not found: " + condAttr);
                        return Result.FAILURE;
                    }
                } else {
                    System.out.println("Unsupported condition format: " + condition);
                    return Result.FAILURE;
                }
            }

            // Print header for the selected columns.
            List<String> outputHeader = new ArrayList<>();
            for (int index : outputIndices) {
                outputHeader.add(columns[index]);
            }

            Logger.logResult("\n");
            Logger.logResult(String.join("\t", outputHeader));
            // Process each row
            String line;
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\t");
                boolean include = true;
                if (conditionColumn != -1) {
                    // Evaluate condition on the specific column.
                    String cellValue = row.length > conditionColumn ? row[conditionColumn] : "";
                    include = evaluateCondition(cellValue, condOperator, condValue);
                }
                if (include) {
                    List<String> outputRow = new ArrayList<>();
                    for (int index : outputIndices) {
                        outputRow.add(row.length > index ? row[index] : "");
                    }
                    Logger.logResult("\n");
                    Logger.logResult(String.join("\t", outputRow));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Result.SUCCESS;
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
        if (!tableFile.exists()) {
            System.out.println("Table does not exist: " + tableName);
            return Result.FAILURE;
        }
        try {
            List<String> lines = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
                String headerLine = br.readLine();
                if (headerLine == null) {
                    System.out.println("Table is empty.");
                    return Result.FAILURE;
                }
                lines.add(headerLine); // preserve header
                String[] columns = headerLine.split("\t");

                // Parse the condition.
                int conditionColumn = -1;
                String condOperator = null;
                String condValue = null;
                if (condition != null && !condition.isEmpty()) {
                    Pattern conditionPattern = Pattern.compile(
                            "([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE
                    );
                    Matcher matcher = conditionPattern.matcher(condition);
                    if (matcher.matches()) {
                        String condAttr = matcher.group(1);
                        condOperator = matcher.group(2);
                        condValue = matcher.group(3).trim();
                        // Remove surrounding quotes if present.
                        condValue = condValue.replaceAll("^'|'$", "");
                        // Find the column index.
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].equalsIgnoreCase(condAttr)) {
                                conditionColumn = i;
                                break;
                            }
                        }
                        if (conditionColumn == -1) {
                            System.out.println("Condition attribute not found: " + condAttr);
                            return Result.FAILURE;
                        }
                    } else {
                        System.out.println("Unsupported condition format: " + condition);
                        return Result.FAILURE;
                    }
                }

                // Process rows and keep only those rows that do NOT match the condition.
                String line;
                while ((line = br.readLine()) != null) {
                    String[] row = line.split("\t");
                    boolean deleteRow = false;
                    if (conditionColumn != -1) {
                        String cellValue = row.length > conditionColumn ? row[conditionColumn] : "";
                        deleteRow = evaluateCondition(cellValue, condOperator, condValue);
                    }
                    if (!deleteRow) {
                        lines.add(line);
                    }
                }
            }
            // Write the remaining lines back to the file.
            try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile))) {
                for (String l : lines) {
                    pw.println(l);
                }
            }
            System.out.println("Deleted matching rows from table " + tableName);
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    public Result alterTable(String tableName, String alterationType, String attributeName) {
        //TODO helper func for below conditions
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);
        if (!tableFile.exists()) {
            System.out.println("Table does not exist: " + tableName);
            return Result.FAILURE;
        }
        try {
            List<String[]> rows = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
                String headerLine = br.readLine();
                // TODO helper func for table
                if (headerLine == null) {
                    System.out.println("Table is empty.");
                    return Result.FAILURE;
                }
                String[] header = headerLine.split("\t");
                rows.add(header);
                String line;
                while ((line = br.readLine()) != null) {
                    // Preserve trailing empty columns by using -1 in split.
                    String[] row = line.split("\t", -1);
                    rows.add(row);
                }
            }
            if (alterationType.equalsIgnoreCase("ADD")) {
                // Check if column exists.
                String[] header = rows.get(0);
                for (String col : header) {
                    if (col.equalsIgnoreCase(attributeName)) {
                        System.out.println("Column already exists: " + attributeName);
                        return Result.FAILURE;
                    }
                }
                // Append new column to header.
                String[] newHeader = Arrays.copyOf(rows.get(0), rows.get(0).length + 1);
                newHeader[newHeader.length - 1] = attributeName;
                rows.set(0, newHeader);
                // Append empty value to each data row.
                for (int i = 1; i < rows.size(); i++) {
                    String[] row = rows.get(i);
                    String[] newRow = Arrays.copyOf(row, row.length + 1);
                    newRow[newRow.length - 1] = "";
                    rows.set(i, newRow);
                }
                System.out.println("Added column " + attributeName + " to table " + tableName);
            } else if (alterationType.equalsIgnoreCase("DROP")) {
                // Remove the specified column.
                String[] header = rows.get(0);
                int dropIndex = -1;
                for (int i = 0; i < header.length; i++) {
                    if (header[i].equalsIgnoreCase(attributeName)) {
                        dropIndex = i;
                        break;
                    }
                }
                if (dropIndex == -1) {
                    System.out.println("Column not found: " + attributeName);
                    return Result.FAILURE;
                }
                String[] newHeader = new String[header.length - 1];
                for (int i = 0, j = 0; i < header.length; i++) {
                    if (i != dropIndex) {
                        newHeader[j++] = header[i];
                    }
                }
                rows.set(0, newHeader);
                for (int i = 1; i < rows.size(); i++) {
                    String[] row = rows.get(i);
                    if (row.length <= dropIndex) continue;
                    String[] newRow = new String[row.length - 1];
                    for (int j = 0, k = 0; j < row.length; j++) {
                        if (j != dropIndex) {
                            newRow[k++] = row[j];
                        }
                    }
                    rows.set(i, newRow);
                }
                System.out.println("Dropped column " + attributeName + " from table " + tableName);
            } else {
                System.out.println("Unsupported alteration type: " + alterationType);
                return Result.FAILURE;
            }
            // Write updated content back to the table file.
            //TODO write helper func for below code
            try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile))) {
                for (String[] row : rows) {
                    pw.println(String.join("\t", row));
                }
            }
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    public Result updateTable(String tableName, Map<String, String> nameValuePairs, String condition) {
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }
        File tableFile = getTableFile(tableName);
        if (!tableFile.exists()) {
            System.out.println("Table does not exist: " + tableName);
            return Result.FAILURE;
        }
        try {
            List<String[]> rows = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
                String headerLine = br.readLine();
                if (headerLine == null) {
                    System.out.println("Table is empty.");
                    return Result.FAILURE;
                }
                String[] header = headerLine.split("\t");
                rows.add(header);
                String line;
                while ((line = br.readLine()) != null) {
                    // Use -1 to preserve trailing empty fields.
                    String[] row = line.split("\t", -1);
                    rows.add(row);
                }
            }
            // Determine column indices to update.
            String[] header = rows.get(0);
            Map<Integer, String> updateIndices = new HashMap<>();
            for (Map.Entry<String, String> entry : nameValuePairs.entrySet()) {
                String colName = entry.getKey();
                boolean found = false;
                for (int i = 0; i < header.length; i++) {
                    if (header[i].equalsIgnoreCase(colName)) {
                        updateIndices.put(i, entry.getValue());
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    System.out.println("Column not found: " + colName);
                    return Result.FAILURE;
                }
            }
            // Parse condition, if provided.
            int conditionColumn = -1;
            String condOperator = null;
            String condValue = null;
            if (condition != null && !condition.isEmpty()) {
                // Expecting format: [AttributeName] <Comparator> [Value]
                Pattern conditionPattern = Pattern.compile(
                        "([a-zA-Z_][a-zA-Z0-9_]*)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)", Pattern.CASE_INSENSITIVE);
                Matcher matcher = conditionPattern.matcher(condition);
                if (matcher.matches()) {
                    String condAttr = matcher.group(1);
                    condOperator = matcher.group(2);
                    condValue = matcher.group(3).trim();
                    // Remove surrounding quotes if present.
                    condValue = condValue.replaceAll("^'|'$", "");
                    // Find condition attribute index.
                    for (int i = 0; i < header.length; i++) {
                        if (header[i].equalsIgnoreCase(condAttr)) {
                            conditionColumn = i;
                            break;
                        }
                    }
                    if (conditionColumn == -1) {
                        System.out.println("Condition attribute not found: " + condAttr);
                        return Result.FAILURE;
                    }
                } else {
                    System.out.println("Unsupported condition format: " + condition);
                    return Result.FAILURE;
                }
            }
            // Process each data row (starting from index 1; index 0 is header).
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);
                boolean updateRow = true;
                if (conditionColumn != -1) {
                    String cellValue = row.length > conditionColumn ? row[conditionColumn] : "";
                    updateRow = evaluateCondition(cellValue, condOperator, condValue);
                }
                if (updateRow) {
                    // Update specified columns.
                    for (Map.Entry<Integer, String> entry : updateIndices.entrySet()) {
                        int colIndex = entry.getKey();
                        if (colIndex < row.length) {
                            row[colIndex] = entry.getValue();
                        }
                    }
                }
            }
            // Write updated rows back to the file.
            try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile))) {
                for (String[] row : rows) {
                    pw.println(String.join("\t", row));
                }
            }
            System.out.println("Updated table " + tableName);
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    public Result joinTables(String tableName1, String tableName2, String attributeName1, String attributeName2) {
        if (isDatabaseInactive()) {
            System.out.println("No database selected");
            return Result.FAILURE;
        }

        File tableFile1 = getTableFile(tableName1);
        File tableFile2 = getTableFile(tableName2);

        if (!tableFile1.exists() || !tableFile2.exists()) {
            System.out.println("Table does not exist: " + tableName1);
            return Result.FAILURE;
        }

        try {
            // Read table1 rows.
            List<String[]> table1Rows = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile1))) {
                String headerLine = br.readLine();
                if (headerLine == null) {
                    System.out.println("Table " + tableName1 + " is empty.");
                    return Result.FAILURE;
                }
                String[] header1 = headerLine.split("\t");
                table1Rows.add(header1);
                String line;
                while ((line = br.readLine()) != null) {
                    table1Rows.add(line.split("\t", -1));
                }
            }

            // Read table2 rows.
            List<String[]> table2Rows = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(tableFile2))) {
                String headerLine = br.readLine();
                if (headerLine == null) {
                    System.out.println("Table " + tableName2 + " is empty.");
                    return Result.FAILURE;
                }
                String[] header2 = headerLine.split("\t");
                table2Rows.add(header2);
                String line;
                while ((line = br.readLine()) != null) {
                    table2Rows.add(line.split("\t", -1));
                }
            }

            // Assume id column is the first column (index 0). We discard it.
            // Locate join attribute indices in each table (search starting from index 1).
            String[] header1 = table1Rows.get(0);
            String[] header2 = table2Rows.get(0);

            // Determine join attribute indices.
            int joinIndex1;
            if (attributeName1.equalsIgnoreCase("id")) {
                joinIndex1 = indexOf(header1, attributeName1, 0);
            } else {
                joinIndex1 = indexOf(header1, attributeName1, 1);
            }
            if (joinIndex1 == -1) {
                System.out.println("Join attribute " + attributeName1 + " not found in table " + tableName1);
                return Result.FAILURE;
            }

            int joinIndex2;
            if (attributeName2.equalsIgnoreCase("id")) {
                joinIndex2 = indexOf(header2, attributeName2, 0);
            } else {
                joinIndex2 = indexOf(header2, attributeName2, 1);
            }
            if (joinIndex2 == -1) {
                System.out.println("Join attribute " + attributeName2 + " not found in table " + tableName2);
                return Result.FAILURE;
            }

            // Build joined header.
            List<String> joinedHeader = new ArrayList<>();
            joinedHeader.add("id"); // New unique id column.
            // For table1, include columns (from index 1 onward) excluding the join attribute.
            for (int i = 1; i < header1.length; i++) {
                if (i == joinIndex1) continue;
                joinedHeader.add(tableName1 + "." + header1[i]);
            }
            // For table2, include columns (from index 1 onward) excluding the join attribute.
            for (int i = 1; i < header2.length; i++) {
                if (i == joinIndex2) continue;
                joinedHeader.add(tableName2 + "." + header2[i]);
            }
            Logger.logResult("\n");
            Logger.logResult(String.join("\t", joinedHeader));

            // Nested-loop join.
            int newId = 1;
            for (int i = 1; i < table1Rows.size(); i++) {
                String[] row1 = table1Rows.get(i);
                if (row1.length <= joinIndex1) continue;
                String joinValue1 = row1[joinIndex1];
                for (int j = 1; j < table2Rows.size(); j++) {
                    String[] row2 = table2Rows.get(j);
                    if (row2.length <= joinIndex2) continue;
                    String joinValue2 = row2[joinIndex2];
                    if (joinValue1.equals(joinValue2)) {
                        List<String> joinedRow = new ArrayList<>();
                        joinedRow.add(String.valueOf(newId));
                        // Add columns from table1 (excluding id and join attribute).
                        for (int k = 1; k < row1.length; k++) {
                            if (k == joinIndex1) continue;
                            joinedRow.add(row1[k]);
                        }
                        // Add columns from table2 (excluding id and join attribute).
                        for (int k = 1; k < row2.length; k++) {
                            if (k == joinIndex2) continue;
                            joinedRow.add(row2[k]);
                        }
                        Logger.logResult("\n");
                        Logger.logResult(String.join("\t", joinedRow));
                        newId++;
                    }
                }
            }
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

    private int indexOf(String[] header, String attribute, int startIndex) {
        for (int i = startIndex; i < header.length; i++) {
            if (header[i].equalsIgnoreCase(attribute)) {
                return i;
            }
        }
        return -1;
    }

}


