package com.aks.dataset;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 *
 * @author Aksenov Sergey
 */
public class Factory {

    private static DataSource ds = null;

    static {
        try {
            Context initContext = new InitialContext();
            ds = (DataSource) initContext.lookup("java:/comp/env/jdbc/school-sr");
            //ds = (DataSource) initContext.lookup("java:/comp/env/jdbc/rono5");
        } catch (NamingException e) {
            System.out.println(e);
        }
    }

    public static Connection getConnection() {
        try {
            Connection conn = ds.getConnection();
            return conn;
        } catch (SQLException e) {
            System.out.println(e);
            return null;
        }
    }

    public static Connection getConnection(boolean f) {
        try {
            Connection conn = ds.getConnection();
            conn.setAutoCommit(f);
            return conn;
        } catch (SQLException e) {
            System.out.println(e);
            return null;
        }
    }

    public static Statement getStatementQuery(Connection connection) {
        try {
            return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            System.err.println(e);
            return null;
        }
    }
}
