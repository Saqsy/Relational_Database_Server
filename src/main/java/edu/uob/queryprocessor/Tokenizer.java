package edu.uob.queryprocessor;

import java.util.List;

public class Tokenizer {
    private final List<Token> tokens;
    private int position;

    public Tokenizer(String input) {
        Lexer lexer = new Lexer(input);
        this.tokens = lexer.tokenize();
        this.position = 0;
    }

    /**
     * Get the current token
     * @return Current token
     */
    public Token getCurrentToken() {
        if (position >= tokens.size()) {
            return tokens.get(tokens.size() - 1); // Return the EOF token
        }
        return tokens.get(position);
    }

    /**
     * Peek at the next token without consuming it
     * @return Next token
     */
    public Token peekNextToken() {
        if (position + 1 >= tokens.size()) {
            return tokens.get(tokens.size() - 1); // Return the EOF token
        }
        return tokens.get(position + 1);
    }

    /**
     * Advance to the next token
     * @return Next token
     */
    public Token nextToken() {
        if (position >= tokens.size() - 1) {
            return tokens.get(tokens.size() - 1); // Return the EOF token
        }
        return tokens.get(++position);
    }

    /**
     * Check if the current token matches the expected type
     * @param type Expected token type
     * @return True if the current token matches the expected type
     */
    public boolean match(TokenType type) {
        if (getCurrentToken().getType() == type) {
            nextToken();
            return true;
        }
        return false;
    }

    /**
     * Expect the current token to be of the specified type
     * @param type Expected token type
     * @throws RuntimeException if the token doesn't match
     */
    public void expect(TokenType type) {
        if (!match(type)) {
            Token current = getCurrentToken();
            throw new RuntimeException("Unexpected token");
        }
    }

    /**
     * Reset the tokenizer position
     */
    public void reset() {
        position = 0;
    }

    /**
     * Check if all tokens have been consumed
     * @return True if all tokens have been consumed
     */
    public boolean isAtEnd() {
        return getCurrentToken().getType() == TokenType.END;
    }

    /**
     * Get all tokens
     * @return List of all tokens
     */
    public List<Token> getTokens() {
        return tokens;
    }
}
