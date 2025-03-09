package edu.uob.dbengine;

import edu.uob.outputprocessor.Logger;
import edu.uob.outputprocessor.Result;

public class DatabaseHandler {

    private String query;
    private DatabaseOperationHandler operationHandler;

    public DatabaseHandler() {
        operationHandler = new DatabaseOperationHandler();
    }

    public void parseQuery(String query) {
        QueryParser queryParser = new QueryParser(operationHandler,query);
        if (queryParser.parse() == Result.SUCCESS) {
            Logger.insertLog(0, Result.SUCCESS.value);
        } else {
            Logger.insertLog(0, Result.FAILURE.value);
        }
    }

}
