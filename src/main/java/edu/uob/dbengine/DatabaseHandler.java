package edu.uob.dbengine;

import edu.uob.outputprocessor.Logger;

public class DatabaseHandler {

    private String query;
    private DatabaseOperationHandler operationHandler;

    public DatabaseHandler() {
        operationHandler = new DatabaseOperationHandler();
    }

    public void parseQuery(String query) {
        QueryParser queryParser = new QueryParser(operationHandler,query);
        Logger.logResult(queryParser.parse().value);
    }

}
