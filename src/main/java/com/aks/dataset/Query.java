package com.aks.dataset;

import com.aks.dataset.Field.TYPE;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.swing.JOptionPane;

/**
 *
 * @author Aksenov Sergey
 *
 * <p>
 * Запросы к данным в базе </p>
 */
public class Query extends Table {

    private String schema = "";
    private static DataSource datasource = null;
    private Connection connection = null;
    protected static int countException;
    public static boolean nullable = false;
    
    public static String INS = "INS";
    public static String SEL = "SEL";
    public static String UPD = "UPD";
    public static String DEL = "DEL";

    private HashMap<String, ArrayList> metaHm;
    public static LinkedHashSet<Query> listOpenTable = new LinkedHashSet<Query>();

    /*static {
        try {
            Context initContext = new InitialContext();
            datasource = (DataSource) initContext.lookup("java:/comp/env/jdbc/empty");
        } catch (NamingException e) {
            System.out.println(e);
        }
    }*/
    
    public Query(Field... fields) {
        super(fields);
    }
    
    public Query(Connection connection) {
        super();
        this.connection = connection;
    }

    public Query(Connection connection, Field... fields) {
        super(fields);
        this.connection = connection;
    }

    public Query select(String sql) {

        if (sql == null) {
            //System.out.println(sql);
            fields[0].selectSql();
            return this;
        }
        try {
            clear();
            sql = param(sql);
            //System.out.println(sql);
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet recordset = statement.executeQuery(sql);
            ResultSetMetaData md = recordset.getMetaData();
            //arrIndexField =  [[индекс_enum, индекс_поля], [индекс_enum, индекс_поля]... ]
            ArrayList<Integer[]> arrIndexField = new ArrayList();
            //поля не вошедшие в eEnum
            ArrayList<Integer> arrColumn = new ArrayList();
            //максимальное число полей eEnum
            int indexMax = 0;
            for (Field field : fields) {
                indexMax = (field.ordinal() > indexMax) ? field.ordinal() : indexMax;
            }
            //цикл по полям ResultSetMetaData
            for (int indexColumn = 1; indexColumn <= md.getColumnCount(); indexColumn++) {
                String fieldTable = md.getColumnLabel(indexColumn); //имя поля
                boolean isPush = false;

                //цикл по полям текущей eEnum
                for (Field fieldEnum : fields) {

                    //если имена полей совпадают                        
                    if (fieldEnum.meta().field_name.equalsIgnoreCase(fieldTable)) {

                        //размер поля данных
                        fieldEnum.meta().size(md.getColumnDisplaySize(indexColumn));
                        //добавим индексы текущего поля
                        Integer[] indexField = {fieldEnum.ordinal(), indexColumn};
                        arrIndexField.add(indexField);
                        //установим соответствие  имя поля DataSet и индекс Enum.field
                        columnToIndex.put(fieldEnum.name(), fieldEnum.ordinal());
                        isPush = true;
                        break;
                    }
                }
                //если попаданий не было
                if (isPush == false) {
                    //дополнительные поля Record
                    arrColumn.add(indexColumn);
                    //установим соответствие  имя поля DataSet и индекс Enum.field
                    columnToIndex.put(fieldTable, indexMax + arrColumn.size());
                }
            }
            //заполним Query данными   
            if (recordset.first() == true) {
                do {
                    //добавим строку в таблицу
                    Record record = newRecord();
                    record.setNo(0, SEL);
                    //цикл по полям
                    for (Integer[] indexField : arrIndexField) {
                        record.setNo(indexField[0], recordset.getObject(indexField[1]));
                    }
                    //дополнительные поля
                    for (Integer indexColumn : arrColumn) {
                        record.add(recordset.getObject(indexColumn));
                    }
                    add(record);

                } while (recordset.next());
            }
            return this;

        } catch (SQLException e) {
            System.out.println(fields[0].ntable() + ".select() " + e);
            System.out.println(sql);
            return null;
        }
    }

    public Query[] select(String sql, Field[]... fieldList) {

        String nameEnum = "";
        try {
            sql = param(sql);
            //System.out.println(sql);
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet recordset = statement.executeQuery(sql);
            ResultSetMetaData md = recordset.getMetaData();
            Query[] queryList = new Query[fieldList.length]; //////////////////////////////////////////надо подправить!!!!!!
            //hmField = [индекс_таблицы: [[индекс_enum, индекс_поля], [индекс_enum, индекс_поля]... ]]
            HashMap<Integer, ArrayList<Integer[]>> hmField = new HashMap();
            //цикл по таблицам, fieldsList.length - количество таблиц участвующих в запросе
            for (int indexEnum = 0; indexEnum < fieldList.length; indexEnum++) {

                Field[] fieldEnums = fieldList[indexEnum]; //поля текущей eEnum       
                nameEnum = fieldEnums[0].ntable(); //имя eEnum     

                hmField.put(indexEnum, new ArrayList());
                queryList[indexEnum] = new Query(connection, fieldEnums);

                //цикл по полям ResultSetMetaData
                for (int indexColumn = 1; indexColumn <= md.getColumnCount(); indexColumn++) {

                    String nameTable = md.getTableName(indexColumn); //имя таблицы                    

                    //если поле принадлежит таблице
                    if (nameEnum.equalsIgnoreCase(nameTable)) {
                        String fieldTable = md.getColumnLabel(indexColumn); //имя поля

                        //цикл по полям текущей eEnum
                        for (Field fieldEnum : fieldEnums) {

                            //если имена полей совпадают                        
                            if (fieldEnum.meta().field_name.equalsIgnoreCase(fieldTable)) {

                                //размер поля данных
                                fieldEnum.meta().size(md.getColumnDisplaySize(indexColumn));
                                //добавим индексы текущего поля
                                ArrayList<Integer[]> arrIndexField = hmField.get(indexEnum);
                                Integer[] indexField = {fieldEnum.ordinal(), indexColumn};
                                arrIndexField.add(indexField);
                            }
                        }
                    }
                }
            }
            //заполним Query данными    
            //цикл по таблицам, fieldsList.length - количество таблиц участвующих в запросе
            for (int indexTable = 0; indexTable < fieldList.length; indexTable++) {

                Query query = queryList[indexTable];
                if (recordset.first() == true) {
                    do {
                        //добавим строку в таблицу
                        Record record = query.newRecord();
                        query.add(record);
                        record.setNo(0, SEL);

                        for (Map.Entry<Integer, ArrayList<Integer[]>> entry : hmField.entrySet()) {
                            if (entry.getKey().equals(indexTable)) {

                                ArrayList<Integer[]> arrIndexField = entry.getValue();
                                for (Integer[] indexField : arrIndexField) {
                                    record.setNo(indexField[0], recordset.getObject(indexField[1]));
                                }
                            }
                        }

                    } while (recordset.next());
                }
                queryList[indexTable] = query;
            }
            return queryList;

        } catch (SQLException e) {
            System.out.println(fields[0].ntable() + ".select() " + e);
            System.out.println(sql);
            return null;
        }
    }

    public int insert(Record record) throws SQLException {
        return insert(record, true);
    }

    public int insert(Record record, boolean nullable) throws SQLException {
        if (Query.INS.equals(record.get(fields[0])) == false) {
            return 0;
        }
        String sql = fields[0].insertSql(record);
        Statement statement = connection.createStatement();
        //если есть insert утверждение
        if (sql != null) {
            return statement.executeUpdate(sql);
        } else {
            //если нет, генерю сам
            String nameCols = "", nameVals = "";
            //цикл по полям таблицы
            for (int k = 1; k < fields.length; k++) {
                Field field = fields[k];
                if (field.meta().type() != TYPE.OBJ) {
                    if (nullable == false) {
                        if (record.get(field) != null) {
                            nameCols = nameCols + field.name() + ",";
                            nameVals = nameVals + wrapperVal(record, field) + ",";
                        }
                    } else {
                        nameCols = nameCols + field.name() + ",";
                        nameVals = nameVals + wrapperVal(record, field) + ",";
                    }
                }
            }
            if (nameCols != null && nameVals != null) {
                nameCols = nameCols.substring(0, nameCols.length() - 1);
                nameVals = nameVals.substring(0, nameVals.length() - 1);
                sql = "insert into " + schema + fields[0].ntable() + "(" + nameCols + ") values(" + nameVals + ")";
                System.out.println(sql);
                return statement.executeUpdate(sql);
            }
        }
        return 0;
    }

    public int update(Record record) throws SQLException {
        if (Query.UPD.equals(record.get(fields[0])) == false) {
            return 0;
        }
        String sql = "empty";
        Statement statement = connection.createStatement();
        sql = fields[0].updateSql(record);
        if (sql != null) {
            //eExcep.printSql(sql);
            return statement.executeUpdate(sql);
        } else {
            String nameCols = "";
            //цикл по полям таблицы
            for (int k = 1; k < fields.length; k++) {
                Field field = fields[k];
                if (field.meta().type() != TYPE.OBJ) {
                    if (nullable == false) {
                        //if (record.get(field) != null) {
                        nameCols = nameCols + field.name() + " = " + wrapperVal(record, field) + ",";
                        //}
                    } else {
                        nameCols = nameCols + field.name() + " = " + wrapperVal(record, field) + ",";
                    }
                }
            }
            if (nameCols.isEmpty() == false) {
                nameCols = nameCols.substring(0, nameCols.length() - 1);
                sql = "update " + schema + fields[0].ntable() + " set " + nameCols + " where " + fields[1].name() + " = " + wrapperVal(record, fields[1]);
                System.out.println(sql);
                return statement.executeUpdate(sql);
            }
        }
        return 0;
    }

    public int delete(Record record) throws SQLException {
        /*if (Query.DEL.equals(record.get(fields[0])) == false) {
            return 0;
        }*/
        String sql = "empty";
        Statement statement = connection.createStatement();
        sql = fields[0].deleteSql(record);
        if (sql != null) {
            //System.out.println(sql);
            return statement.executeUpdate(sql);
        } else {
            sql = "delete from " + schema + fields[0].ntable() + " where " + fields[1].name() + " = " + wrapperVal(record, fields[1]);
            System.out.println(sql);
            return statement.executeUpdate(sql);
        }
    }

    public String wrapperVal(Record record, Field field) {
        try {
            if (record.get(field) == null) {
                return null;
            } else if (TYPE.STR.equals(field.meta().type())) {
                return "'" + record.getStr(field) + "'";
            } else if (TYPE.BOOL.equals(field.meta().type())) {
                return "'" + record.getStr(field) + "'";
            } else if (TYPE.DATE.equals(field.meta().type())) {
                if (record.get(field) instanceof java.util.Date) {
                    return " '" + new SimpleDateFormat("dd.MM.yyyy").format(record.getDate(field)) + "' ";
                } else {
                    return " '" + record.getStr(field) + "' ";
                }
            }
            return record.getStr(field);
        } catch (Exception e) {
            System.out.println("Query.vrapper() " + e);
            return null;
        }
    }

    /**
     * Каскадное обновления таблиц
     */
    public static void autoselectSql() {
        for (Query rs : listOpenTable) {
            rs.select(null);
        }
    }

    /**
     * Каскадное обновления таблиц
     */
    public static boolean autoexecSql() throws SQLException {
        countException = 0;
        //список таблиц которые обновляются только при перезагрузке программы
        String table_name[] = {}; //{eUchType.table_name, eUchVid.table_name, eDict1.table_name, eKladr1.table_name, eKladr2.table_name};

        for (Query query : listOpenTable) {
            boolean exists = false;
            for (String it : table_name) {
                if (it.equals(query.fields()[0].ntable()) == true) {
                    exists = true;
                }
            }
            if (query.fields() != null && exists == false) {
                try {
                    query.executeSql();
                } catch (SQLException e) {
                    throw new SQLException(e);
                }
            }
        }
        //FieldEditor.setClickCountToStart(2);
        Record.DIRTY = false;
        return countException == 0;
    }

    public void executeSql() throws SQLException {

        String mes = null;
        for (int indexTable = 0; indexTable < size(); ++indexTable) {

            Record record = get(indexTable);
            if (record.get(0).equals(Query.UPD)
                    || record.get(0).equals(INS)) {

                //eExcep.printSql(record);
                //проверка на корректность ввода данных
                mes = record.validate(fields);
                if (mes != null) {
                    JOptionPane.showMessageDialog(null, mes, "Предупреждение", JOptionPane.INFORMATION_MESSAGE);
                    ++countException;
                    return;
                }
            }
            if ("INS".equals(record.getStr(0))) {
                insert(record);
                record.setNo(0, Query.SEL);
            } else if ("UPD".equals(record.getStr(0))) {
                update(record);
                record.setNo(0, Query.SEL);
            } else if ("DEL".equals(record.getStr(0))) {
                delete(record);
                remove(indexTable--);
            }
        }
    }

    public void executeSQL(Object records, Field... fields) throws SQLException {
        try {
            Context initContext = new InitialContext();
            DataSource dataSource = (DataSource) initContext.lookup("java:/comp/env/jdbc/school-sr");
            Connection connection = dataSource.getConnection();
            ArrayList<HashMap> records2 = (ArrayList) records;
            if (records != null && records2.isEmpty() == false) {

                Query query = new Query(connection, fields);
                for (HashMap hm : records2) {

                    Record record = query.newRecord();
                    for (Field field : fields) {
                        record.setNo(field, hm.get(field.name()));
                    }

                    if ("INS".equals(record.getStr(0))) {
                        insert(record);
                    } else if ("UPD".equals(record.getStr(0))) {
                        update(record);
                    } else if ("DEL".equals(record.getStr(0))) {
                        delete(record);
                    }
                }
            }
        } catch (Exception v) {
        }
    }

    public Query schema(String schema) {
        this.schema = schema;
        return this;
    }

    private String param(String sql) {

        try {
            String param[] = {"${scheme}", "${p1}", "${p2}", "${p3}"};
            for (int i = 0; i < param.length; i++) {
                int mark = sql.indexOf(param[i]);
                if (mark != -1) {

                    if ("${p1}".equals(param[i])) {
                        sql = sql.replace(param[i], "xxx");

                    } else if ("${p2}".equals(param[i])) {
                        sql = sql.replace(param[i], "yyy");

                    } else if ("${p3}".equals(param[i])) {
                        sql = sql.replace(param[i], "zzz");
                    }
                }
            }
            return sql;

        } catch (Exception e) {
            System.err.println("Query.param - " + e);
            return null;
        }
    }

    public int generatorId(int step) {
        String table_name = fields[0].ntable();
        String tag_name = "";
        int next_id = 0;
        try {
            Statement statement = connection.createStatement();
            String mySeqv = table_name + "_id_seq";
            ResultSet rs = statement.executeQuery("SELECT nextval('" + mySeqv + "')");
            if (rs.next()) {
                next_id = rs.getInt(step);
            }
            rs.close();
            return next_id;
        } catch (SQLException e) {
            System.out.println("Ошибка генерации ключа " + e);
            return 0;
        }
    }

    public static Connection getConnection() {
        try {
            Connection conn = datasource.getConnection();
            return conn;
        } catch (SQLException e) {
            System.out.println(e);
            return null;
        }
    }

    public boolean equals(Object obj) {
        return (this == obj);
    }
}
