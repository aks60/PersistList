package com.aks.model;

import com.aks.dataset.Entity;
import com.aks.dataset.Query;
import com.aks.dataset.Record;
import com.aks.domain.eTable1;
import com.aks.domain.eTable2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Aksenov Sergey
 */
public class Main {

    public static void main(String[] args) {

        Main main = new Main();
        main.execute();
    }

    public void test() {

        eTable1 et = eTable1.id;
        eTable1[] q = eTable1.values();
        Entity ant = Entity.type;
        Object o1 = Entity.type.ordinal();
        Query qTable1 = new Query(eTable1.values());
    }

    public void execute() {
        Connection conn = null;
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection("jdbc:derby:jdbcTest;create=true", "", "");
            Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = conn.getMetaData().getTables(null, null, "TABLE%", null);
            if (rs.next() == false) {

                st.execute("CREATE TABLE Table1 (id integer, name1 varchar(80), empty_col varchar(80), PRIMARY KEY (id))");
                st.execute("CREATE TABLE Table2 (id integer, name2 varchar(80), table1_id integer, PRIMARY KEY (id))");
            }
            st.executeUpdate("delete from Table1");
            st.executeUpdate("delete from Table2");
            st.executeUpdate("INSERT INTO Table1 VALUES (1, 'name1-1', 'xxx')");
            st.executeUpdate("INSERT INTO Table1 VALUES (2, 'name1-2', 'vvv')");
            st.executeUpdate("INSERT INTO Table1 VALUES (3, 'name1-3', 'mmm')");
            st.executeUpdate("INSERT INTO Table2 VALUES (1, 'name2-1', 1)");
            st.executeUpdate("INSERT INTO Table2 VALUES (2, 'name2-2', 3)");
            st.executeUpdate("INSERT INTO Table2 VALUES (3, 'name2-3', 3)");

            System.out.println();
            System.out.println("PART №1 - SELECT, INSERT, UPDATE, DELETE К ОДНОЙ ТАБЛИЦЕ.");            
            Query qTable1 = new Query(conn, eTable1.values());
            Query query1 = qTable1.select("select a.* from table1 a where a.id < 10");            
            Record record1 = qTable1.newRecord();
            record1.set(eTable1.up, Query.INS);
            record1.set(eTable1.id, 4);
            record1.set(eTable1.name1, "xxx");
            Record record2 = query1.get(0);
            record2.set(eTable1.name1, "zzz");
            qTable1.insert(record1);       // insert            
            qTable1.delete(query1.get(1)); // delete
            qTable1.update(record2);       // update            
            query1.stream().forEach(rec -> System.out.println(rec.getStr(eTable1.id) + " " + rec.getStr(eTable1.name1) + " " + rec.get("empty_col")));
            conn.commit();

            System.out.println();
            System.out.println("PART №2 - SELECT, INSERT, UPDATE К ДВУМ ТАБЛИЦАМ."); 
            Query[] query2 = new Query(conn).select("select a.*, b.* from table1 a, table2 b where a.id = b.table1_id", eTable1.values(), eTable2.values());            
            for (int index = 0; index < query2[0].size(); index++) {  

                System.out.println(query2[0].get(index) + " : " + query2[1].get(index));
            }
            Record record3 = query2[1].newRecord();
            record3.set(eTable1.up, Query.INS);
            record3.set(eTable1.id, 5);
            record3.set(eTable1.name1, "bbb");
            record3.set(3, "nnn");
            query2[0].insert(record3);                  //insert
            query2[1].get(2).set(eTable2.name2, "vvv"); //update
            query2[1].executeSql();            
            conn.commit();

        } catch (ClassNotFoundException e) {
            
            System.err.println(e);
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.err.println(e2);
            }
            System.err.println(e);
        }
    }
}
