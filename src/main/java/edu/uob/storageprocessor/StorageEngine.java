package edu.uob.storageprocessor;

import edu.uob.exceptions.DatabaseOperationException;

import java.io.*;

public class StorageEngine {

    BufferedReader br = null;
    PrintWriter pw = null;

    public void getFileWriter(File file) throws DatabaseOperationException {
        try {
            pw = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        } catch (IOException e) {
            throw new DatabaseOperationException(" Error occurred while writing data");
        }
    }

    public void getFileReader(File file) throws DatabaseOperationException {
        if (file == null || !file.exists()) {
            throw new DatabaseOperationException("Table does not exist: " + file.getName());
        }
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new DatabaseOperationException(" Error occurred while opening data file");
        }
    }

    public String readLine() throws DatabaseOperationException {
        if (br == null) {
            throw new DatabaseOperationException(" Error occurred while reading file");
        }
        try {
            return br.readLine();
        } catch (IOException e) {
            throw new DatabaseOperationException(" Error occurred while reading data file");
        }
    }

    public void writeData(String data) throws DatabaseOperationException {
        if (pw == null) {
            throw new DatabaseOperationException(" Error occurred while writing data");
        }
        pw.println(data);
    }

    public void flushWriter() throws DatabaseOperationException {
        try {
            br.close();
        } catch (IOException e) {
            throw new DatabaseOperationException(" Error occurred while writing data");
        }
    }

    public void flushReader() throws DatabaseOperationException {
        pw.close();
    }

}
