package edu.uob.queryprocessor;

import edu.uob.exceptions.InvalidSyntaxException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to Lex input string into tokens
 *
 * @author Saquib Kazi
 */
public class Lexer {

    // Regular expression patterns for token matching
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("^\\s+");
    private static final Pattern KEYWORD_PATTERN = Pattern.compile("^(?i)(USE|CREATE|DATABASE|TABLE|DROP|ALTER|INSERT|INTO|VALUES|SELECT|FROM|WHERE|UPDATE|SET|DELETE|JOIN|AND|OR|ON|ADD|LIKE|NULL|TRUE|FALSE)(?![a-zA-Z0-9_])");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("^[+-]?\\d+");
    private static final Pattern FLOAT_PATTERN = Pattern.compile("^[+-]?\\d+\\.\\d+");
    private static final Pattern STRING_PATTERN = Pattern.compile("^'[^']*'");
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("^(;|\\(|\\)|,|==|>=|<=|!=|=|>|<|\\*)");


    private final String input;
    private int position = 0;

    public Lexer(String input) {
        this.input = input;
    }


    public List<Token> tokenize() throws InvalidSyntaxException {
        List<Token> tokens = new ArrayList<>();
        Token token;

        while ((token = nextToken()).getType() != TokenType.LINE_END) {
            if (token.getType() != TokenType.WHITESPACE) {
                tokens.add(token);
            }
        }

        List<Token> endTokens = tokens.stream()
                .filter(t -> t.getType() == TokenType.END)
                .toList();

        if (endTokens.isEmpty()) {
            throw new InvalidSyntaxException(" Invalid syntax: ';' is expected");
        } else if (endTokens.size() > 1) {
            throw new InvalidSyntaxException(" Invalid syntax: more than one ';' found");
        }

        return tokens;
    }


    public Token nextToken() throws InvalidSyntaxException {
        if (position >= input.length()) {
            return new Token(TokenType.LINE_END, "");
        }

        String remainingInput = input.substring(position);

        // Match whitespace
        Matcher whitespaceMatcher = WHITESPACE_PATTERN.matcher(remainingInput);
        if (whitespaceMatcher.find()) {
            String whitespace = whitespaceMatcher.group();
            advance(whitespace.length());
            return new Token(TokenType.WHITESPACE, whitespace);
        }

        // Match keywords
        Matcher keywordMatcher = KEYWORD_PATTERN.matcher(remainingInput);
        if (keywordMatcher.find()) {
            String keyword = keywordMatcher.group().toUpperCase();
            advance(keyword.length());

            // Handle boolean literals
            if (keyword.equals("TRUE") || keyword.equals("FALSE")) {
                return new Token(TokenType.BOOLEAN_LITERAL, keyword);
            }

            // Handle NULL
            if (keyword.equals("NULL")) {
                return new Token(TokenType.NULL, keyword);
            }

            // Map keyword string to token type
            TokenType type;
            try {
                type = TokenType.valueOf(keyword);
            } catch (IllegalArgumentException e) {
                // Should never happen since we're matching against defined keywords
                throw new InvalidSyntaxException("Unrecognized keyword: " + keyword);
            }
            return new Token(type, keyword);
        }

        // Match identifiers
        Matcher identifierMatcher = IDENTIFIER_PATTERN.matcher(remainingInput);
        if (identifierMatcher.find()) {
            String identifier = identifierMatcher.group();
            advance(identifier.length());
            return new Token(TokenType.IDENTIFIER, identifier);
        }

        // Match float literals (must come before integer literals)
        Matcher floatMatcher = FLOAT_PATTERN.matcher(remainingInput);
        if (floatMatcher.find()) {
            String floatLiteral = floatMatcher.group();
            advance(floatLiteral.length());
            return new Token(TokenType.FLOAT_LITERAL, floatLiteral);
        }

        // Match integer literals
        Matcher integerMatcher = INTEGER_PATTERN.matcher(remainingInput);
        if (integerMatcher.find()) {
            String intLiteral = integerMatcher.group();
            advance(intLiteral.length());
            return new Token(TokenType.INTEGER_LITERAL, intLiteral);
        }

        // Match string literals
        Matcher stringMatcher = STRING_PATTERN.matcher(remainingInput);
        if (stringMatcher.find()) {
            String stringLiteral = stringMatcher.group().substring(1, stringMatcher.group().length() - 1);
            advance(stringLiteral.length() + 2);
            return new Token(TokenType.STRING_LITERAL, stringLiteral);
        }

        // Match symbols
        Matcher symbolMatcher = SYMBOL_PATTERN.matcher(remainingInput);
        if (symbolMatcher.find()) {
            String symbol = symbolMatcher.group();
            advance(symbol.length());

            switch (symbol) {
                case ";":
                    return new Token(TokenType.END, symbol);
                case "(":
                    return new Token(TokenType.LEFT_PAREN, symbol);
                case ")":
                    return new Token(TokenType.RIGHT_PAREN, symbol);
                case ",":
                    return new Token(TokenType.COMMA, symbol);
                case "=":
                    return new Token(TokenType.EQUALS, symbol);
                case ">":
                    return new Token(TokenType.GREATER_THAN, symbol);
                case "<":
                    return new Token(TokenType.LESS_THAN, symbol);
                case ">=":
                    return new Token(TokenType.GREATER_EQUALS, symbol);
                case "<=":
                    return new Token(TokenType.LESS_EQUALS, symbol);
                case "!=":
                    return new Token(TokenType.NOT_EQUALS, symbol);
                case "==":
                    return new Token(TokenType.EQUALS, symbol);
                case "*":
                    return new Token(TokenType.ASTERISK, symbol);
                default:
                    throw new InvalidSyntaxException(" Unrecognized symbol: " + symbol);
            }
        }

        char unrecognizedChar = remainingInput.charAt(0);
        throw new InvalidSyntaxException(" Unrecognized character: " + unrecognizedChar);
    }

    private void advance(int amount) {
        for (int i = 0; i < amount; i++) {
            position++;
        }
    }

}
