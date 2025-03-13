package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseServerTests {
    private DBServer server;

    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    private String generateRandomName() {
        String randomName = "";
        for(int i=0; i<10 ;i++) randomName += (char)( 97 + (Math.random() * 25.0));
        return randomName;
    }

    private String sendCommandToServer(String command) {
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> { return server.handleCommand(command);},
                "Server took too long to respond (probably stuck in an infinite loop)");
    }

    @Test
    public void testCreateDatabase() {
        String randomName = generateRandomName();
        String response = sendCommandToServer("CREATE DATABASE " + randomName + ";");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("USE " + randomName + ";");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
    }

    @Test
    public void testInsertQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass", lines.get(1));
        assertEquals("1\tSimon\t65\tTRUE", lines.get(2));
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
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass", lines.get(1));
        assertEquals("3\tRob\t35\tFALSE", lines.get(2));
        assertEquals("4\tChris\t20\tFALSE", lines.get(3));
        assertEquals(4, lines.size());
        response = sendCommandToServer("SELECT * FROM marks where mark < 50;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass", lines.get(1));
        assertEquals("3\tRob\t35\tFALSE", lines.get(2));
        assertEquals("4\tChris\t20\tFALSE", lines.get(3));
        assertEquals(4, lines.size());
        response = sendCommandToServer("SELECT * FROM marks WHERE (pass == FALSE) AND (mark < 25);");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("4\tChris\t20\tFALSE", lines.get(2));
        assertEquals(3, lines.size());
        response = sendCommandToServer("SELECT * FROM marks WHERE name LIKE 'i';");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("1\tSimon\t65\tTRUE", lines.get(2));
        assertEquals("2\tSion\t55\tTRUE", lines.get(3));
        assertEquals("4\tChris\t20\tFALSE", lines.get(4));
        assertEquals(5, lines.size());
    }

    @Test
    public void testUpdateQuery(){
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        String response = sendCommandToServer("UPDATE marks SET mark = 38 WHERE name == 'Sion';");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("SELECT name,mark FROM marks where name == 'Sion';");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("Sion\t38", lines.get(2));
        response = sendCommandToServer("UPDATE marks SET mark = 50 WHERE mark < 100;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("SELECT name,mark FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("Simon\t50", lines.get(2));
        assertEquals("Sion\t50", lines.get(3));
    }

    @Test
    public void testAlterQuery(){
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        String response = sendCommandToServer("ALTER TABLE marks ADD age;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("SELECT * FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tpass\tage", lines.get(1));
        response = sendCommandToServer("ALTER TABLE marks DROP pass;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("SELECT * FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tname\tmark\tage", lines.get(1));
        assertEquals("1\tSimon\t65\t", lines.get(2));
    }

    @Test
    public void testJoinQuery(){
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        sendCommandToServer("CREATE TABLE coursework (task, submission);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO',3);");
        sendCommandToServer("INSERT INTO coursework VALUES ('DB',1);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO',4);");
        sendCommandToServer("INSERT INTO coursework VALUES ('STAG',2);");
        String response = sendCommandToServer("JOIN coursework AND marks ON submission AND id;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id\tcoursework.task\tmarks.name\tmarks.mark\tmarks.pass", lines.get(1));
        assertEquals("1\tOXO\tRob\t35\tFALSE", lines.get(2));
        assertEquals("2\tDB\tSimon\t65\tTRUE", lines.get(3));
        assertEquals("3\tOXO\tChris\t20\tFALSE", lines.get(4));
        assertEquals("4\tSTAG\tSion\t55\tTRUE", lines.get(5));
    }

    @Test
    public void testDeleteQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE coursework (task, submission);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO',3);");
        sendCommandToServer("INSERT INTO coursework VALUES ('DB',1);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO',4);");
        sendCommandToServer("INSERT INTO coursework VALUES ('STAG',2);");
        String response = sendCommandToServer("DELETE FROM coursework WHERE task == 'OXO';");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("SELECT * FROM coursework;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("2\tDB\t1", lines.get(2));
        assertEquals("4\tSTAG\t2", lines.get(3));
        assertEquals(4, lines.size());
    }

    @Test
    public void testDropQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("DROP TABLE marks;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("Select * from marks;");
        lines = response.lines().toList();
        assertEquals("[ERROR] Table not found", lines.get(0));
        response = sendCommandToServer("DROP DATABASE " + randomName + ";");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        response = sendCommandToServer("USE " + randomName + ";");
        lines = response.lines().toList();
        assertEquals("[ERROR] Database doesn't exist", lines.get(0));
    }

    @Test
    public void testDataPersistAfterRestart() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));

        // Restart Server a.k.a creating a new object
        server = new DBServer();

        // Testing if we can still get data from folder
        sendCommandToServer("USE " + randomName + ";");
        response = sendCommandToServer("SELECT * FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
    }

    @Test
    public void testBasicInvalidSyntax() {
        String randomName = generateRandomName();
        //Semicolon
        String response = sendCommandToServer("CREATE DATABASE " + randomName);
        assertEquals("[ERROR] Invalid syntax: ';' is expected", response);

        //Create
        response = sendCommandToServer("CREATE " + randomName + ";");
        assertEquals("[ERROR] Invalid type after CREATE Keyword", response);
        response = sendCommandToServer(" DATABASE " + randomName + ";");
        assertEquals("[ERROR] Invalid command type: DATABASE", response);
        response = sendCommandToServer("CREATE DATABASE  ;");
        assertEquals("[ERROR] Invalid identifier", response);
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        response = sendCommandToServer("CREATE TABLE marks ;");
        assertEquals("[ERROR] Column Names not passed", response);

        //Insert
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        response = sendCommandToServer("INSERT INTO marks VALUES ;");
        assertEquals("[ERROR] Unexpected syntax", response);

        //SELECT
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        response = sendCommandToServer("Select * from No table;");
        assertEquals("[ERROR] Table not found", response);
        response = sendCommandToServer("Select column from marks;");
        assertEquals("[ERROR] Column not found", response);
        response = sendCommandToServer("Select * from marks where name;");
        assertEquals("[ERROR] Invalid condition format: name", response);
    }

    @Test
    public void testAdvanceQuerySyntax() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");

        //Alter
        String response = sendCommandToServer("ALTER TABLE marks age;");
        assertEquals("[ERROR] Missing alteration type", response);
        response = sendCommandToServer("ALTER TABLE marks ADD ;");
        assertEquals("[ERROR] Missing attribute name", response);

        //UPDATE
        response = sendCommandToServer("UPDATE marks age = 35;");
        assertEquals("[ERROR] Unexpected syntax", response);
        response = sendCommandToServer("UPDATE marks SET age = 35;");
        assertEquals("[ERROR] Unexpected syntax", response);
        response = sendCommandToServer("UPDATE marks SET age = 35 WHERE name = 'chris';");
        assertEquals("[ERROR] Unsupported condition format: name = chris", response);
        response = sendCommandToServer("UPDATE marks SET age = 35 WHERE name == 'chris';");
        assertEquals("[ERROR] Update failed value not found: name == chris", response);
        response = sendCommandToServer("UPDATE marks SET invalidCol = 35 WHERE name == 'Simon';");
        assertEquals("[ERROR] Update failed column doesn't exist: invalidCol", response);

        //DROP
        response = sendCommandToServer("DROP Something marks;");
        assertEquals("[ERROR] Invalid syntax in drop statement", response);
        response = sendCommandToServer("DROP TABLE max;");
        assertEquals("[ERROR] Table not found", response);
        response = sendCommandToServer("DROP DATABASE max;");
        assertEquals("[ERROR] Database does not exist: max", response);
    }

    @Test
    public void testTableId() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");

        // Delete from top
        sendCommandToServer("DELETE FROM marks WHERE name == 'Simon';");
        String response = sendCommandToServer("SELECT id FROM marks;");
        List<String> lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id", lines.get(1));
        assertEquals("2", lines.get(2));
        assertEquals("3", lines.get(3));
        assertEquals("4", lines.get(4));

        // Add at bottom
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        response = sendCommandToServer("SELECT id FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id", lines.get(1));
        assertEquals("2", lines.get(2));
        assertEquals("3", lines.get(3));
        assertEquals("4", lines.get(4));
        assertEquals("5", lines.get(5));

        // Delete from middle
        sendCommandToServer("DELETE FROM marks WHERE id == 3;");
        response = sendCommandToServer("SELECT id FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id", lines.get(1));
        assertEquals("2", lines.get(2));
        assertEquals("4", lines.get(3));
        assertEquals("5", lines.get(4));

        // Add at bottom
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        response = sendCommandToServer("SELECT id FROM marks;");
        lines = response.lines().toList();
        assertEquals("[OK]", lines.get(0));
        assertEquals("id", lines.get(1));
        assertEquals("2", lines.get(2));
        assertEquals("4", lines.get(3));
        assertEquals("5", lines.get(4));
        assertEquals("6", lines.get(5));

    }
}
