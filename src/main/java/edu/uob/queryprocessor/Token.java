package edu.uob.queryprocessor;

public class Token {
    private final String value;
    private final TokenType type;

    Token(TokenType type,String value) {
        this.value = value;
        this.type = type;
    }

    public TokenType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Token{" + "value='" + value + '\'' + ", type=" + type + '}';
    }
}
