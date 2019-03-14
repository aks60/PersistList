package com.aks.domain;

import com.aks.dataset.Field;
import com.aks.dataset.MetaField;
import com.aks.dataset.Record;

public enum eTable1 implements Field {

    up("0", "0", "0", "Activity"),
    id("4", "10", "0", "id"),
    name1("12", "80", "0", "Name");
    
    private MetaField meta = new MetaField(this);
    public static String table_name = "Table1";
    
    eTable1(Object... p) {
        meta.init(p);
    }

    public void selectSql() {
        //here we write sql
    }

    public String updateSql(Record record) {
        return null;//here we write sql
    }

    public String insertSql(Record record) {
        return null;//here we write sql
    }

    public String deleteSql(Record record) {
        return null;//here we write sql
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
