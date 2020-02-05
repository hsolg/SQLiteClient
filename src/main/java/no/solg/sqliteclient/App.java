package no.solg.sqliteclient;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.sqlite.SQLiteConfig;

public class App {
    static void executeQuery(String sql, Connection conn) {
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                int count = meta.getColumnCount();
                for (int i=1; i<=count; i++) {
                    String val = rs.getString(i);
                    System.out.print(val + " ");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void listTables(Connection conn) {
        try {
            ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
            while (rs.next()) {
                System.out.println(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static String getDatabaseName(String databasePath) {
        final String filename = new File(databasePath).getName();
        if (filename.indexOf('.') != -1) {
            return filename.substring(0, filename.indexOf('.'));
        } else {
            return filename;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: sqlitequery.jar <database file>");
        } else {
            final String dbPath = args[0];
            final String dbName = getDatabaseName(dbPath);

            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            try {
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath, config.toProperties());

                Terminal terminal = TerminalBuilder.terminal();
                LineReader lineReader = LineReaderBuilder.builder()
                        .terminal(terminal)
                        .build();
                final String prompt = dbName + "> ";
                while (true) {
                    try {
                        final String sql = lineReader.readLine(prompt);
                        if (sql.equalsIgnoreCase(".quit")) {
                            break;
                        } else if (sql.equalsIgnoreCase(".tables")) {
                            listTables(conn);
                        } else if (sql.startsWith(".")) {
                            System.out.println("Unknown command: " + sql);
                        } else {
                            executeQuery(sql, conn);
                        }
                    } catch (UserInterruptException e) {
                        break;
                    } catch (EndOfFileException e) {
                        break;
                    }
                }
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
