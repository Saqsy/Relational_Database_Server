package edu.uob.queryprocessor;

import edu.uob.exceptions.InvalidSyntaxException;

import java.util.List;

public class Tokenizer {
    private final List<Token> tokens;
    private int position;

    public Tokenizer(String input) throws InvalidSyntaxException {
        Lexer lexer = new Lexer(input);
        this.tokens = lexer.tokenize();
        this.position = 0;
    }

    public Token getCurrentToken() {
        if (position >= tokens.size()) {
            return tokens.get(tokens.size() - 1);
        }
        return tokens.get(position);
    }

    public Token peekNextToken() {
        if (position + 1 >= tokens.size()) {
            return tokens.get(tokens.size() - 1);
        }
        return tokens.get(position + 1);
    }

    public Token nextToken() {
        if (position >= tokens.size() - 1) {
            return tokens.get(tokens.size() - 1); // Return the EOF token
        }
        return tokens.get(++position);
    }

    public boolean match(TokenType type) {
        return getCurrentToken().getType() == type;
    }

    public void expect(TokenType type) throws InvalidSyntaxException {
        expect(type, " Unexpected syntax");
    }

    public void expect(TokenType type, String error) throws InvalidSyntaxException {
        if (!match(type)) {
            throw new InvalidSyntaxException(error);
        }
    }

    public void reset() {
        position = 0;
    }

    public boolean isAtEnd() {
        return getCurrentToken().getType() == TokenType.END;
    }

    public List<Token> getTokens() {
        return tokens;
    }
}
