package edu.uob.queryprocessor;

public enum TokenType {
        USE, CREATE, DATABASE, TABLE, DROP, ALTER, INSERT, INTO, VALUES,
        SELECT, FROM, WHERE, UPDATE, SET, DELETE, JOIN, AND, OR, ON, ADD, LIKE, NULL,

        // Literals
        INTEGER_LITERAL, FLOAT_LITERAL, STRING_LITERAL, BOOLEAN_LITERAL,

        // Identifiers
        IDENTIFIER,

        // Symbols
        SEMICOLON, LEFT_PAREN, RIGHT_PAREN, COMMA, EQUALS,
        GREATER_THAN, LESS_THAN, GREATER_EQUALS, LESS_EQUALS, NOT_EQUALS,

        // Special
        ASTERISK, WHITESPACE, END, LINE_END
}
