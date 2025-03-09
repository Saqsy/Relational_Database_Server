package edu.uob.queryprocessor;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Lexer {

    // Reserved keywords list
    private static final Set<String> RESERVED_KEYWORDS = new HashSet<>(Arrays.asList(
            "USE", "CREATE", "DATABASE", "TABLE", "DROP", "ALTER", "INSERT", "INTO",
            "VALUES", "SELECT", "FROM", "WHERE", "SET", "UPDATE", "DELETE", "JOIN", "AND", "ON", "ADD", "LIKE"
    ));

    // Regular expression patterns for token matching
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("^\\s+");
    private static final Pattern KEYWORD_PATTERN = Pattern.compile("^(?i)(USE|CREATE|DATABASE|TABLE|DROP|ALTER|INSERT|INTO|VALUES|SELECT|FROM|WHERE|UPDATE|SET|DELETE|JOIN|AND|OR|ON|ADD|LIKE|NULL|TRUE|FALSE)(?![a-zA-Z0-9_])");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("^[+-]?\\d+");
    private static final Pattern FLOAT_PATTERN = Pattern.compile("^[+-]?\\d+\\.\\d+");
    private static final Pattern STRING_PATTERN = Pattern.compile("^'[^']*'");
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("^(;|\\(|\\)|,|==|>=|<=|!=|=|>|<|\\*)");


    private final String input;
    private final List<Token> Tokens = new ArrayList<>();
    private int position = 0;
    private int line = 1;
    private int linePosition = 0;

    public Lexer(String input) {
        this.input = input;
    }

    /**
     * Tokenize the entire input string
     * @return List of tokens
     */
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token;

        while ((token = nextToken()).getType() != TokenType.END) {
            if (token.getType() != TokenType.WHITESPACE) {
                tokens.add(token);
            }
        }
        tokens.add(token); // Add EOF token
        return tokens;
    }

    /**
     * Get the next token from the input
     * @return Next token
     */
    public Token nextToken() {
        if (position >= input.length()) {
            return new Token(TokenType.END, "");
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
                throw new RuntimeException("Unrecognized keyword: " + keyword);
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
            //TODO handle it better to remove leading and trailing single qoutes '
            String stringLiteral = stringMatcher.group().substring(1, stringMatcher.group().length() - 1);
            advance(stringLiteral.length()+2);
            return new Token(TokenType.STRING_LITERAL, stringLiteral);
        }

        // Match symbols
        Matcher symbolMatcher = SYMBOL_PATTERN.matcher(remainingInput);
        if (symbolMatcher.find()) {
            String symbol = symbolMatcher.group();
            advance(symbol.length());

            // Return the appropriate token based on the symbol
            switch (symbol) {
                case ";": return new Token(TokenType.SEMICOLON, symbol);
                case "(": return new Token(TokenType.LEFT_PAREN, symbol);
                case ")": return new Token(TokenType.RIGHT_PAREN, symbol);
                case ",": return new Token(TokenType.COMMA, symbol);
                case "=": return new Token(TokenType.EQUALS, symbol);
                case ">": return new Token(TokenType.GREATER_THAN, symbol);
                case "<": return new Token(TokenType.LESS_THAN, symbol);
                case ">=": return new Token(TokenType.GREATER_EQUALS, symbol);
                case "<=": return new Token(TokenType.LESS_EQUALS, symbol);
                case "!=": return new Token(TokenType.NOT_EQUALS, symbol);
                case "==": return new Token(TokenType.EQUALS, symbol);
                case "*": return new Token(TokenType.ASTERISK, symbol);
                default: throw new RuntimeException("Unrecognized symbol: " + symbol);
            }
        }

        // If we reach here, we have an unrecognized character
        char unrecognizedChar = remainingInput.charAt(0);
        int startPos = linePosition;
        advance(1);
        throw new RuntimeException("Unrecognized character at line " + line + ", position " + startPos + ": " + unrecognizedChar);
    }

    /**
     * Advance the position by the specified amount
     * @param amount Amount to advance
     */
    private void advance(int amount) {
        for (int i = 0; i < amount; i++) {
            char c = input.charAt(position);
            if (c == '\n') {
                line++;
                linePosition = 0;
            } else {
                linePosition++;
            }
            position++;
        }
    }
    
    
    public List<Token> getTokens() {
        return Tokens;
    }
}
