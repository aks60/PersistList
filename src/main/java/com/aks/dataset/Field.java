package com.aks.dataset;

import java.util.Date;
import java.util.HashSet;


/**
 *
 * @author Aksenov Sergey
 * 
 * Поле таблицы
 */
public interface Field {

    public static enum TYPE {

        OBJ(Object.class, 0), INT(Integer.class, 4, 5), NPP(Integer.class), INTsp(Integer.class),
        INTfp(Integer.class), STR(String.class, -9, -1, 1, 12), DBL(Double.class, 8), FLT(Float.class, 6), LONG(Long.class, -5),
        DATE(Date.class, 91, 92, 93), BOOL(Boolean.class, 16), BLOB(String.class, -2, -3, -4, 2004);
        Class type;
        HashSet<Integer> conv = new HashSet();

        TYPE(Class type, Integer... v) {
            this.type = type;
            for (Integer i : v) {
                conv.add(i);
            }
        }
    };

    public static enum DOMAIN {

        NULL(0), S70000(70000), S80000(80000), S100000(100000), S110000(110000), 
        S190000(190000),S200000(200000), S210000(210000), S220000(220000), 
        S230000(230000),S240000(240000), S250000(250000), S260000(260000), 
        S270000(270000),S280000(280000), S290000(290000), S300000(300000), 
        S310000(310000),S320000(320000), S330000(330000), S340000(340000), 
        S350000(350000),S360000(360000), S370000(370000), S380000(380000), 
        S390000(390000),S410000(410000), S420000(420000), S440000(440000), 
        S450000(450000),S460000(460000), S470000(470000), S480000(480000), 
        S500000(500000), S510000(510000),S520000(520000), S530000(530000), 
        S550000(550000), S570000(570000),S590000(590000), S600000(600000), 
        S610000(610000), S790000(790000),S800000(800000), S810000(810000), 
        S820000(820000), S830000(830000),S840000(840000), S850000(850000), 
        S860000(860000), S870000(870000),S880000(880000), S890000(890000), 
        S900000(900000), S910000(910000),S920000(920000), S930000(930000), 
        S950000(950000), S970000(970000),S1000000(1000000), S1010000(1010000), 
        S1030000(1030000), S1070000(1070000),S1040000(1040000), S1050000(1050000), 
        S1080000(1080000), S1100000(1100000),S1110000(1110000), S1120000(1120000), 
        S1160000(1160000), S1180000(1180000),S1190000(1190000), S1200000(1200000), 
        S1210000(1210000), S1220000(1220000),S1230000(1230000), S1240000(1240000), 
        S1320000(1320000), S1330000(1330000),S1340000(1340000), S1350000(1350000), 
        S1140000(1140000), S1150000(1150000),S1250000(1250000), S1310000(1310000), 
        S1360000(1360000), S2020000(2020000),S2040000(2040000), S2050000(2050000), 
        S2060000(2060000), S2080000(2080000), S2200000(2200000), S2220000(2220000), 
        S2230000(2230000), S2240000(2240000);
        Integer domain;

        DOMAIN(int domain) {
            this.domain = domain;
        }
    };

    public static enum EDIT {

        TRUE(true), FALSE(false);
        Boolean edit;

        EDIT(boolean value) {
            this.edit = value;
        }
    };
    
    public void selectSql();

    public String updateSql(Record record);

    public String insertSql(Record record);

    public String deleteSql(Record record);

    public String ntable();

    public Field[] fields();

    public int ordinal();

    public String name();

    public MetaField meta();

    //public Object value();

   // public void value(Object value);
    
    //public Field valueOf(String name);

    public boolean equals(Object other);
    
    //public Query table();  
}
