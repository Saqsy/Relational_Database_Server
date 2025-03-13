package edu.uob.dbengine;

import edu.uob.exceptions.DatabaseOperationException;
import edu.uob.exceptions.InvalidCommandException;
import edu.uob.exceptions.InvalidSyntaxException;
import edu.uob.outputprocessor.Logger;
import edu.uob.outputprocessor.Result;
import edu.uob.queryprocessor.Token;
import edu.uob.queryprocessor.TokenType;
import edu.uob.queryprocessor.Tokenizer;

import java.util.*;

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
            Logger.logResult(" : Query is null");
            return Result.FAILURE;
        } else {
            try {
                tokenizer = new Tokenizer(query);
                return parseCommandType();
            } catch (InvalidSyntaxException | InvalidCommandException | RuntimeException |
                     DatabaseOperationException e) {
                Logger.logResult(e.getMessage());
                return Result.FAILURE;
            }
        }
    }

    private Result parseCommandType() throws InvalidCommandException, InvalidSyntaxException, DatabaseOperationException {
        Token token = tokenizer.getCurrentToken();

        if (token == null) {
            return Result.FAILURE;
        }

        return switch (token.getValue().toUpperCase()) {
            case "USE" -> parseUse();
            case "CREATE" -> parseCreate();
            case "DROP" -> parseDrop();
            case "ALTER" -> parseAlter();
            case "INSERT" -> parseInsert();
            case "SELECT" -> parseSelect();
            case "UPDATE" -> parseUpdate();
            case "DELETE" -> parseDelete();
            case "JOIN" -> parseJoin();
            default -> {
                throw new InvalidCommandException(" Invalid command type: " + token.getValue());
            }
        };
    }

    private Result parseUse() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // USE
        TokenType type = tokenizer.getCurrentToken().getType();
        if (type != TokenType.IDENTIFIER) {
            throw new InvalidSyntaxException(" Expected Identifier after USE Keyword");
        }
        Token dbName = tokenizer.getCurrentToken();
        return operationHandler.useDatabase(dbName.getValue());
    }

    private Result parseCreate() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // CREATE
        TokenType type = tokenizer.getCurrentToken().getType();
        boolean isValidType = (type == TokenType.DATABASE || type == TokenType.TABLE);
        boolean isNextTokenValid = tokenizer.peekNextToken().getType() != TokenType.IDENTIFIER;
        if (!isValidType) {
            throw new InvalidSyntaxException(" Invalid type after CREATE Keyword");
        } else if (isNextTokenValid) {
            throw new InvalidSyntaxException(" Invalid identifier");
        }

        if (type == TokenType.DATABASE) {
            Token DBname = tokenizer.nextToken();
            return operationHandler.createDatabase(DBname.getValue());
        } else {
            Token tableName = tokenizer.nextToken();
            List<String> attributes = new ArrayList<>();
            Token token = tokenizer.nextToken();
            if (token != null && token.getType() == TokenType.LEFT_PAREN) {
                attributes = parseAttributeList();
                Token closing = tokenizer.nextToken(); // )
                if (closing.getType() != TokenType.RIGHT_PAREN) {
                    throw new InvalidSyntaxException(" Missing closing parenthesis in CREATE TABLE");
                }
            }
            return operationHandler.createTable(tableName.getValue(), attributes);
        }
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

    private Result parseInsert() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // INSERT
        tokenizer.expect(TokenType.INTO);
        tokenizer.nextToken(); // INTO
        tokenizer.expect(TokenType.IDENTIFIER);
        Token tableName = tokenizer.getCurrentToken();
        tokenizer.nextToken(); // VALUES
        tokenizer.expect(TokenType.VALUES);
        tokenizer.nextToken(); // (
        tokenizer.expect(TokenType.LEFT_PAREN);
        List<String> values = parseValueList();
        if (values.isEmpty()) {
            throw new InvalidSyntaxException(" Missing values");
        }
        tokenizer.nextToken(); // )
        tokenizer.expect(TokenType.RIGHT_PAREN);
        return operationHandler.insertIntoTable(tableName.getValue(), values);
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

    private Result parseSelect() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // SELECT
        List<String> attributes = parseWildAttributeList();
        if (attributes.isEmpty()) {
            throw new InvalidSyntaxException(" Missing attributes");
        }
        tokenizer.nextToken(); //FROM
        tokenizer.expect(TokenType.FROM);
        Token tableName = tokenizer.nextToken(); // Tablename
        tokenizer.expect(TokenType.IDENTIFIER);
        String condition = null;
        Token token = tokenizer.nextToken(); // WHERE
        if (token != null && token.getType() == TokenType.WHERE) {
            tokenizer.nextToken(); // WHERE
            condition = parseCondition();
        }
        return operationHandler.selectFromTable(tableName.getValue(), attributes, condition);
    }

    private String parseCondition() {
        StringBuilder condition = new StringBuilder();
        while (tokenizer.getCurrentToken() != null && tokenizer.getCurrentToken().getType() != TokenType.END) {
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

    private Result parseDrop() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); //DROP
        Token next = tokenizer.getCurrentToken();
        if (next != null && next.getType() == TokenType.DATABASE) {
            Token DBname = tokenizer.nextToken();
            return operationHandler.dropDatabase(DBname.getValue().trim());
        } else if (next != null && next.getType() == TokenType.TABLE) {
            Token tableName = tokenizer.nextToken();
            return operationHandler.dropTable(tableName.getValue().trim());
        }
        throw new InvalidSyntaxException(" Invalid syntax in drop statement");
    }

    private Result parseDelete() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // DELETE
        tokenizer.expect(TokenType.FROM);
        tokenizer.nextToken(); // FROM
        tokenizer.expect(TokenType.IDENTIFIER);
        Token tableName = tokenizer.getCurrentToken();
        tokenizer.nextToken(); // TableName
        tokenizer.expect(TokenType.WHERE);
        tokenizer.nextToken(); // WHERE
        String condition = parseCondition();
        if (condition.isEmpty()) {
            throw new InvalidSyntaxException(" Missing condition");
        }
        return operationHandler.deleteFromTable(tableName.getValue(), condition);
    }

    private Result parseAlter() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // ALTER
        tokenizer.expect(TokenType.TABLE);
        tokenizer.nextToken(); // TABLE
        tokenizer.expect(TokenType.IDENTIFIER);
        Token tableName = tokenizer.getCurrentToken();
        Token alterationType = tokenizer.nextToken(); // ADD or DROP
        switch (alterationType.getType()) {
            case ADD:
            case DROP:
                break;
            default:
                throw new InvalidSyntaxException(" Missing alteration type");
        }
        Token attributeName = tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Missing attribute name");
        return operationHandler.alterTable(tableName.getValue(), alterationType.getValue(), attributeName.getValue());
    }

    private Result parseUpdate() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // UPDATE
        Token tableName = tokenizer.getCurrentToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Expected table name");
        tokenizer.nextToken(); // SET
        tokenizer.expect(TokenType.SET);
        Map<String, String> nameValuePairs = parseNameValueList();
        tokenizer.nextToken(); // WHERE
        tokenizer.expect(TokenType.WHERE);
        tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER);
        String condition = parseCondition();
        return operationHandler.updateTable(tableName.getValue(), nameValuePairs, condition);
    }

    private Map<String, String> parseNameValueList() throws InvalidSyntaxException {
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

    private String[] parseNameValuePair() throws InvalidSyntaxException {
        Token attributeName = tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER);
        tokenizer.nextToken(); // =
        tokenizer.expect(TokenType.EQUALS);
        Token value = tokenizer.nextToken();
        return new String[]{attributeName.getValue().strip(), value.getValue().strip()};
    }

    private Result parseJoin() throws InvalidSyntaxException, DatabaseOperationException {
        tokenizer.nextToken(); // JOIN
        Token tableName1 = tokenizer.getCurrentToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Missing Table Name");
        tokenizer.nextToken(); // AND
        tokenizer.expect(TokenType.AND);
        Token tableName2 = tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Missing Second Table Name");
        tokenizer.nextToken(); // ON
        tokenizer.expect(TokenType.ON);
        Token attributeName1 = tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Missing attribute Name");
        tokenizer.nextToken(); // AND
        tokenizer.expect(TokenType.AND);
        Token attributeName2 = tokenizer.nextToken();
        tokenizer.expect(TokenType.IDENTIFIER, " Missing second attribute Name");
        return operationHandler.joinTables(tableName1.getValue(), tableName2.getValue(), attributeName1.getValue(), attributeName2.getValue());
    }
}
