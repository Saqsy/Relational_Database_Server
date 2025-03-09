package edu.uob;

import edu.uob.outputprocessor.Result;
import edu.uob.queryprocessor.*;

import java.lang.reflect.Method;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        // Test the tokenizer with some example inputs
        String[] testInputs = {
                "USE database1;",
                "CREATE DATABASE myDb;",
                "CREATE TABLE students (id, name, age);",
                "DROP DATABASE oldDb;",
                "DROP TABLE oldTable;",
                "ALTER TABLE students ADD email;",
                "INSERT INTO students VALUES (1, 'John', 20);",
                "SELECT * FROM students;",
                "SELECT name, age FROM students WHERE age > 18;",
                "UPDATE students SET name = 'Jane' WHERE id = 1;",
                "DELETE FROM students WHERE id = 1;",
                "JOIN students AND courses ON id AND student_id;"
        };

        for (String input : testInputs) {
            System.out.println("\nTokenizing: " + input);
            Tokenizer tokenizer = new Tokenizer(input);
            List<Token> claudeTokens = tokenizer.getTokens();
            for (Token claudeToken : claudeTokens) {
                if (claudeToken.getType() != TokenType.END) {
                    System.out.println(claudeToken);
                }
            }


            // Demonstrate tokenizer usage
            System.out.println("\nDemonstrating tokenizer usage:");
            tokenizer.reset();
            while (!tokenizer.isAtEnd()) {
                Token claudeToken = tokenizer.getCurrentToken();
                System.out.println("Current token: " + claudeToken.getType() + " (" + claudeToken.getValue() + ")");
                tokenizer.nextToken();
            }
            System.out.println("-------------------");
        }
    }
}
