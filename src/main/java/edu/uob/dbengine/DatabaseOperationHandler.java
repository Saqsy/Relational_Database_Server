package edu.uob.dbengine;

import edu.uob.outputprocessor.Logger;
import edu.uob.outputprocessor.Result;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseOperationHandler {

    private static final String DATABASE_DIR = Paths.get("databases").toAbsolutePath().toString();
    private String currentDatabase = null;

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
        try (PrintWriter pw = new PrintWriter(new FileWriter(tableFile, true))) {
            pw.println(String.join("\t", values));
            System.out.println("Inserted into table " + tableName);
            return Result.SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.FAILURE;
    }

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
}


