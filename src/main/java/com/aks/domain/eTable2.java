package com.aks.domain;

import com.aks.dataset.Field;
import com.aks.dataset.MetaField;
import com.aks.dataset.Record;



/**
 * @author aks
 */
public enum eTable2 implements Field {

    up("0", "0", "0", "Activity"),
    id("4", "10", "0", "id"),
    name2("12", "80", "0", "Name"),
    table1_id("4", "10", "0", "id parent");
    
    private MetaField meta = new MetaField(this);
    public static String table_name = "Table2";

    eTable2(Object... p) {
        meta.init(p);
    }

    public void selectSql() {
        //here we write sql
    }

    public String updateSql(Record record) {
        return null; //here we write sql
    }

    public String insertSql(Record record) {
        return null; //here we write sql
    }

    public String deleteSql(Record record) {
        return null; //here we write sql
    }

    public MetaField meta() {
        return meta;
    }

    public Field[] fields() {
        return values();
    }

    public String ntable() {
        return table_name;
    }

    public String toString() {
        return meta.getColName();
    }
}
