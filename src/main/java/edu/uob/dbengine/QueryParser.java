package edu.uob.dbengine;

import edu.uob.outputprocessor.Result;
import edu.uob.queryprocessor.Token;
import edu.uob.queryprocessor.TokenType;
import edu.uob.queryprocessor.Tokenizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParser {

    private DatabaseOperationHandler operationHandler;
    private Tokenizer tokenizer;
    private String query;

    public QueryParser(DatabaseOperationHandler operationHandler, String query) {
        this.operationHandler = operationHandler;
        this.query = query;
    }

    public Result parse() {
        if (query == null) {
            System.out.println("Query is null");
            return Result.FAILURE;
        } else {
            tokenizer = new Tokenizer(query);
        }

        Result queryResult = parseCommandType();
        Token token = tokenizer.getCurrentToken();
        //TODO check if below is necessary
        if (token != null && token.getValue().equals(";")) {
            tokenizer.nextToken();
        }
        return queryResult;
    }

    private Result parseCommandType() {
        Token token = tokenizer.getCurrentToken();
        if (token == null) return Result.FAILURE;
        switch (token.getValue().toUpperCase()) {
            case "USE":
                return parseUse();
            case "CREATE":
                return parseCreate();
            case "DROP":
                return parseDrop();
            case "ALTER":
                return parseAlter();
            case "INSERT":
                return parseInsert();
            case "SELECT":
                return parseSelect();
            case "UPDATE":
                return parseUpdate();
            case "DELETE":
                return parseDelete();
            case "JOIN":
                return parseJoin();
            default:
                System.out.println("Invalid command type: " + token.getValue());
                return Result.FAILURE;
        }
    }

    private Result parseUse() {
        tokenizer.nextToken(); // USE
        TokenType type = tokenizer.getCurrentToken().getType();
        if (type != TokenType.IDENTIFIER) {
            return Result.FAILURE;
        }
        Token dbName = tokenizer.getCurrentToken();
        return operationHandler.useDatabase(dbName.getValue());
    }

    private Result parseCreate() {
        tokenizer.nextToken(); // CREATE
        TokenType type = tokenizer.getCurrentToken().getType();
        boolean isValidType = (type == TokenType.DATABASE || type == TokenType.TABLE);
        boolean isNextTokenValid = tokenizer.peekNextToken().getType() != TokenType.IDENTIFIER;
        if (!isValidType || isNextTokenValid) {
            return Result.FAILURE;
        }

        if (type == TokenType.DATABASE) {
            Token DBname = tokenizer.nextToken();
            return operationHandler.createDatabase(DBname.getValue());
        } else if (type == TokenType.TABLE) {
            //TODO : Check for identifier
            Token tableName = tokenizer.nextToken();
            List<String> attributes = new ArrayList<>();
            Token token = tokenizer.nextToken();
            if (token != null && token.getType() == TokenType.LEFT_PAREN) {
                attributes = parseAttributeList();
                Token closing = tokenizer.nextToken(); // )
                if (closing.getType() != TokenType.RIGHT_PAREN) {
                    System.out.println("Missing closing parenthesis in CREATE TABLE");
                    return Result.FAILURE;
                }
            }
            return operationHandler.createTable(tableName.getValue(), attributes);
        }

        return Result.FAILURE;
    }

    private List<String> parseAttributeList() {
        List<String> attributes = new ArrayList<>();
        Token token = tokenizer.nextToken();
        attributes.add(token.getValue().strip());
        while (tokenizer.peekNextToken() != null && tokenizer.peekNextToken().getType() == TokenType.COMMA) {
            tokenizer.nextToken(); // ,
            Token attr = tokenizer.nextToken();
            attributes.add(attr.getValue().strip());
        }
        return attributes;
    }

    private Result parseInsert() {
        // TODO add type checks for keyword
        tokenizer.nextToken(); // INSERT
        tokenizer.nextToken(); // INTO
        Token tableName = tokenizer.getCurrentToken();
        // TODO type checks
        tokenizer.nextToken(); // VALUES
        tokenizer.nextToken(); // (
        List<String> values = parseValueList();
        tokenizer.nextToken(); // )
        return operationHandler.insertIntoTable(tableName.getValue(),values);
    }

    private List<String> parseValueList() {
        List<String> values = new ArrayList<>();
        Token token = tokenizer.nextToken(); // (
        values.add(token.getValue().strip());
        while (tokenizer.peekNextToken() != null && tokenizer.peekNextToken().getType() == TokenType.COMMA) {
            tokenizer.nextToken(); // ,
            Token value = tokenizer.nextToken();
            values.add(value.getValue().strip());
        }
        return values;
    }

    private Result parseSelect() {
        tokenizer.nextToken(); // SELECT
        List<String> attributes = parseWildAttributeList();
        tokenizer.nextToken(); //FROM
        Token tableName = tokenizer.nextToken(); // Tablename
        String condition = null;
        Token token = tokenizer.nextToken(); // WHERE
        if (token != null && token.getType() == TokenType.WHERE) {
            tokenizer.nextToken(); // WHERE
            condition = parseCondition();
        }
        return operationHandler.selectFromTable(tableName.getValue(),attributes,condition);
    }

    private String parseCondition() {
        //TODO fix condition
        StringBuilder condition = new StringBuilder();
        while(tokenizer.getCurrentToken() != null && tokenizer.getCurrentToken().getType() != TokenType.END) {
            condition.append(tokenizer.getCurrentToken().getValue()).append(" ");
            tokenizer.nextToken();
        }
        return condition.toString().trim();
    }

    private List<String> parseWildAttributeList() {
        List<String> attributes = new ArrayList<>();
        Token token = tokenizer.getCurrentToken();
        if (token != null && token.getType() == TokenType.ASTERISK) {
            attributes.add(token.getValue().strip());
        } else {
            attributes.add(token.getValue().strip());
            while (tokenizer.peekNextToken() != null && tokenizer.peekNextToken().getType() == TokenType.COMMA) {
                tokenizer.nextToken(); // ,
                Token value = tokenizer.nextToken();
                attributes.add(value.getValue().strip());
            }
        }
        return attributes;
    }

    private Result parseDrop() {
        tokenizer.nextToken(); //DROP
        Token next = tokenizer.getCurrentToken();
        if (next != null && next.getType() == TokenType.DATABASE) {
            Token DBname = tokenizer.nextToken();
            return operationHandler.dropDatabase(DBname.getValue().trim());
        } else if (next != null && next.getType() == TokenType.TABLE) {
            Token tableName = tokenizer.nextToken();
            return operationHandler.dropTable(tableName.getValue().trim());
        }
        return Result.FAILURE;
    }

    private Result parseDelete() {
        tokenizer.nextToken(); // DELETE
        tokenizer.nextToken(); // FROM
        Token tableName = tokenizer.getCurrentToken();
        tokenizer.nextToken(); // WHERE
        tokenizer.nextToken(); // WHERE
        String condition = parseCondition();
        return operationHandler.deleteFromTable(tableName.getValue(), condition);
    }

    private Result parseAlter() {
        tokenizer.nextToken(); // ALTER
        tokenizer.nextToken(); // TABLE
        Token tableName = tokenizer.getCurrentToken();
        Token alterationType = tokenizer.nextToken(); // ADD or DROP
        Token attributeName = tokenizer.nextToken();
        return operationHandler.alterTable(tableName.getValue(), alterationType.getValue(), attributeName.getValue());
    }

    private Result parseUpdate() {
        tokenizer.nextToken(); // UPDATE
        Token tableName = tokenizer.getCurrentToken();
        tokenizer.nextToken(); // SET
        Map<String, String> nameValuePairs = parseNameValueList();
        tokenizer.nextToken();
        tokenizer.nextToken(); // WHERE
        String condition = parseCondition();
        return operationHandler.updateTable(tableName.getValue(), nameValuePairs, condition);
    }

    private Map<String, String> parseNameValueList() {
        Map<String, String> nameValuePairs = new HashMap<>();
        String[] pair = parseNameValuePair();
        nameValuePairs.put(pair[0], pair[1]);
        while (tokenizer.peekNextToken() != null && tokenizer.peekNextToken().getType() == TokenType.COMMA) {
            tokenizer.nextToken(); // ,
            pair = parseNameValuePair();
            nameValuePairs.put(pair[0], pair[1]);
        }
        return nameValuePairs;
    }

    private String[] parseNameValuePair() {
        Token attributeName = tokenizer.nextToken();
        tokenizer.nextToken(); // =
        Token value = tokenizer.nextToken();
        return new String[]{attributeName.getValue().strip(), value.getValue().strip()};
    }

    private Result parseJoin() {
        tokenizer.nextToken(); // JOIN
        Token tableName1 = tokenizer.getCurrentToken();
        tokenizer.nextToken(); // AND
        Token tableName2 = tokenizer.nextToken();
        tokenizer.nextToken(); // ON
        Token attributeName1 = tokenizer.nextToken();
        tokenizer.nextToken(); // AND
        Token attributeName2 = tokenizer.nextToken();
        return operationHandler.joinTables(tableName1.getValue(), tableName2.getValue(), attributeName1.getValue(), attributeName2.getValue());
    }
}
