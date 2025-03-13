package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseServerTests {
    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Random name generator - useful for testing "bare earth" queries (i.e. where tables don't previously exist)
    private String generateRandomName() {
        String randomName = "";
        for(int i=0; i<10 ;i++) randomName += (char)( 97 + (Math.random() * 25.0));
        return randomName;
    }

    private String sendCommandToServer(String command) {
        //TODO reduce timeout before submission
        return assertTimeoutPreemptively(Duration.ofMillis(100000), () -> { return server.handleCommand(command);},
                "Server took too long to respond (probably stuck in an infinite loop)");
    }


    @Test
    public void testSelectQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass", lines.get(1));
        assertEquals("1\tSimon\t65\tTRUE", lines.get(2));
        response = sendCommandToServer("SELECT id FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id", lines.get(1));
        assertEquals("1", lines.get(2));
        response = sendCommandToServer("SELECT name,mark FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("name\tmark", lines.get(1));
        assertEquals("Simon\t65", lines.get(2));
    }

    @Test
    public void testSelectWithCondition() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        String response = sendCommandToServer("SELECT name,mark FROM marks where name == Simon;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("name\tmark", lines.get(1));
        assertEquals("Simon\t65", lines.get(2));
        assertEquals(3, lines.size());
        response = sendCommandToServer("SELECT * FROM marks where pass != TRUE;");
        System.out.println(response);
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass", lines.get(1));
        assertEquals("3\tRob\t35\tFALSE", lines.get(2));
        assertEquals("4\tChris\t20\tFALSE", lines.get(3));
        assertEquals(4, lines.size());
    }

}
