package edu.uob.dbengine;

import edu.uob.outputprocessor.Result;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;

public class DatabaseOperationHandler {

    private static final String DATABASE_DIR = Paths.get("databases").toAbsolutePath().toString();
    private String currentDatabase = null;

    private static class CurrentDatabase {
        static String name = null;
        static String path = null;
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
}
