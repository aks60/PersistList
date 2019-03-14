package com.aks.dataset;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Aksenov Sergey
 *
 * <p>
 * Генерация enum </p>
 */
public enum Entity {

    type, size, nullable, comment, fname;

    private static String table = null;
    private static HashMap<String, ArrayList<String>> columns = new HashMap();
    private static ArrayList<String> rangcol = new ArrayList();

    private static void script() {
        System.out.println("");
        System.out.println("package domain;");
        System.out.println("import dataset.Field;");
        System.out.println("import dataset.MetaField;");
        System.out.println("import dataset.Record;");
        System.out.println("import model.sys.Att;");
        System.out.println("public enum e" + table + " implements Field {");
        System.out.println("up(\"0\", \"0\", \"0\", \"0\", \"null\"),");
        int index = 0;
        for (String colname : rangcol) {
            String end = (index++ < columns.size() - 1) ? ")," : ");";
            String str = "";
            for (String attr : columns.get(colname.toLowerCase())) {
                str = str + "\"" + attr + "\",";
            }
            System.out.println(colname.toLowerCase() + "(" + str.substring(0, str.length() - 1) + end);
        }
        System.out.println("private MetaField meta = null;");
        System.out.println("public Object value = null;");
        System.out.println("public static String table_name = \"" + table + "\";");
        System.out.println("e" + table + "(String... p) { meta = new MetaField(p); }");
        System.out.println("public String selectSql(Att att) { return null; }");
        System.out.println("public String updateSql(Record record, Att att) { return null; }");
        System.out.println("public String insertSql(Record record, Att att) { return null; }");
        System.out.println("public String deleteSql(Record record, Att att) { return null; }");
        System.out.println("public MetaField meta() { return meta; }");
        System.out.println("public Field[] fields() { return values(); }");
        System.out.println("public Object value() { return value; }");
        System.out.println("public void value(Object value) { this.value = value; }");
        System.out.println("public String table() {  return table_name;  }");
        System.out.println("}");
    }

    public static void msserver(Connection connection, String _table) {
        try {
            table = _table;
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSetMetaData md = statement.executeQuery("select a.* from " + table + " a").getMetaData();
            Statement statement2 = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for (int col = 1; col <= md.getColumnCount(); col++) {

                rangcol.add(null);
                String colname = md.getColumnName(col).toLowerCase();
                ResultSet rs = statement2.executeQuery("select CAST(sep.value as varchar(256)) as dis from sys.tables st "
                        + "inner join sys.columns sc on st.object_id = sc.object_id "
                        + "left join sys.extended_properties sep on st.object_id = sep.major_id  "
                        + "and sc.column_id = sep.minor_id  and sep.name = 'MS_Description' "
                        + "where st.name = '" + table + "' and  sc.name = '" + colname + "'");
                rs.first();

                ArrayList<String> column = newcol();
                column.set(comment.ordinal(), rs.getString("dis"));
                columns.put(colname, column);
            }
            meta(connection);

        } catch (SQLException e) {
            System.err.println(e);
        }
        script();
    }

    public static void postgres(Connection connection, String _table) {
        try {
            table = _table;
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery("SELECT DISTINCT a.attnum as num, a.attname as name,  format_type(a.atttypid, "
                    + " a.atttypmod) as typ,  a.attnotnull as notnull,  com.description as comment, coalesce(i.indisprimary,false) as primary_key, "
                    + " def.adsrc as default FROM pg_attribute a  JOIN pg_class pgc ON pgc.oid = a.attrelid "
                    + " LEFT JOIN pg_index i ON (pgc.oid = i.indrelid AND i.indkey[0] = a.attnum) LEFT JOIN pg_description com on "
                    + " (pgc.oid = com.objoid AND a.attnum = com.objsubid) LEFT JOIN pg_attrdef def ON  (a.attrelid = def.adrelid "
                    + " AND a.attnum = def.adnum) WHERE a.attnum > 0 AND pgc.oid = a.attrelid AND pg_table_is_visible(pgc.oid) "
                    + " AND NOT a.attisdropped AND pgc.relname = '" + table + "'  ORDER BY a.attnum ");
            while (rs.next()) {
                rangcol.add(null);
                ArrayList<String> column = newcol();
                column.set(comment.ordinal(), rs.getString("comment"));
                columns.put(rs.getString("name"), column);
                //System.out.println(rs.getString("name"));
            }
            meta(connection);

        } catch (SQLException e) {
            System.err.println(e);
        }
        script();
    }

    public static void meta(Connection connection) {
        try {
            DatabaseMetaData md = connection.getMetaData();
            ResultSet rsColumns = md.getColumns(null, null, table, null);
            while (rsColumns.next()) {
                ArrayList<String> column = columns.get(rsColumns.getString("COLUMN_NAME").toLowerCase());
                column.set(type.ordinal(), rsColumns.getString("DATA_TYPE"));
                column.set(size.ordinal(), rsColumns.getString("COLUMN_SIZE"));
                column.set(nullable.ordinal(), rsColumns.getString("NULLABLE"));
                rangcol.set(rsColumns.getInt("ORDINAL_POSITION") - 1, rsColumns.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private static ArrayList<String> newcol() {
        ArrayList<String> column = new ArrayList();
        for (Object en : Entity.values()) {
            column.add(null);
        }
        return column;
    }
}
