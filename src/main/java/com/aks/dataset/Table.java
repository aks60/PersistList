package com.aks.dataset;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 *
 * @author Aksenov Sergey
 *
 * <p>
 * Доступа к строкам таблицы </p>
 */
public class Table extends ArrayList<Record> {

    private static SimpleDateFormat fd = new SimpleDateFormat("dd.MM.yyyy");

    /**
     * Поля вошедшие в Record
     */
    protected Field[] fields = null;

    /**
     * Поля и их индексы вошедшие в Record
     */
    private HashMap<String, Integer> fieldToIndex = new HashMap();

    /**
     * Поля и их индексы
     */
    protected HashMap columnToIndex = new HashMap<String, Integer>() {

        public Integer put(String key, Integer value) {
            return super.put(key.toLowerCase(), value);
        }

        public Integer get(Object key) {
            return (Integer) super.get(key.toString().toLowerCase());
        }
    };

    /**
     * Индекс первичного ключа Record
     */
    private Integer indexPk = null;

    /**
     * Значение первичного ключа и его токен
     */
    private TreeMap<Object, Record> pkToRecord = new TreeMap();

    public Table() {
    }

    public Table(Field... fields) {
        this.fields = fields;
        if (fields[1].name().equals("id") || fields[1].name().equals("sp")) {
            this.indexPk = fields[1].ordinal();
        }
        for (Field field : fields) {
            fieldToIndex.put(field.name(), field.ordinal());
        }
    }

    public Table(Integer indexPk, Field... fields) {
        this.fields = fields;
        this.indexPk = indexPk;
        for (Field field : fields) {
            fieldToIndex.put(field.name(), field.ordinal());
        }
    }

    /**
     * Поля таблицы
     */
    public Field[] fields() {
        return fields;
    }

    public int index(String name) {
        return fieldToIndex.get(name);
    }

    /**
     * Переопределён чтобы отслеживать TreeMap<Object, DataRecord> map
     */
    public Record remove(int index) {
        if (indexPk != null) {
            Record record = get(index);
            pkToRecord.remove(record.get(indexPk));
        }
        return super.remove(index);
    }

    /**
     * Получим новую таблицу фильтруя источник данных
     */
    public Table load(Field... filter) {
        Table rsTable = new Table(fields);
        //загрузка значений ключей фильтра
        Object[] val = new Object[filter.length];
        for (int i = 0; i < val.length; i++) {
            //снимаем со стека
            val[i] = filter[i].meta().pop();
        }
        HashSet<Integer> set = new HashSet<Integer>();
        //цикл по таблице
        for (Record rowTable : this) {
            //если запись удалена
            if (rowTable.get(0).equals(Query.DEL)) {
                continue;
            }
            boolean pass = true;
            boolean compare = true;
            set.clear();
            //цикл по фильтру
            for (int i = 0; i < filter.length; i++) {
                Field field = filter[i];
                //проверка перехода на новый ключ
                boolean add = set.add(field.ordinal());
                //если значение поля таблицы равно null
                if (rowTable.get(field) == null) {
                    if (val[i].equals("null")) {
                        //если ищем нулевое поле
                        compare = true;
                    } else {
                        //дальше искать нет смысла
                        pass = false;
                        break;
                    }
                } else {
                    //тут мы делаем сравнение поля с ключём
                    compare = rowTable.get(field).equals(val[i]);
                }
                //если значения полей <НЕ РАВНЫ>
                //и был переход на новый ключ
                if (compare == false && add == true) {
                    //если до этого промаха не было
                    if (pass == true) {
                        pass = false;
                    } else {
                        //если до этого был промах
                        pass = false;
                        break;
                    }
                } else if (compare == true && add == false) {
                    //иначе если значения <РАВНЫ>
                    //и долбёжка по одному и тому же ключу
                    pass = true;
                }
            }
            //если были только совпадения
            if (pass == true) {
                rsTable.add(rowTable);
            }
        }
        return rsTable;
    }

    /**
     * Найти местоположение записи
     */
    public int locate(Field... field) {
        //заполним массив val[] критериями поиска
        Object[] valField = new Object[field.length];
        for (int i = 0; i < valField.length; i++) {
            valField[i] = field[i].meta().pop();;
        }
        //цикл по записям в таблице
        for (int indexRow = 0; indexRow < size(); ++indexRow) {
            //если запись удалена
            if (get(indexRow, fields()[0]).equals(Query.DEL)) {
                continue;
            }
            //цикл только по полям поиска
            boolean pas = false;
            for (int indexField = 0; indexField < field.length; indexField++) {
                //если значение ключа в поле поиска равно нулю
                if (get(indexRow, field[indexField]) == null) {
                    pas = true;
                    break;
                }
                //если хотябы одно непопадание
                if (!get(indexRow, field[indexField]).equals(valField[indexField])) {
                    pas = true;
                    break;
                }
            }
            //если промахов не было
            if (pas == false) {
                return indexRow;
            }
        }
        return -1;
    }

    /**
     * Добавление данных если из сервера то fields[0] = SELECT если из клиента
     * то fields[0] = INSERT
     */
    public boolean add(Record record) {
        //Record record = isAl(elements);
        if (indexPk != null) {
            Object valuePk = record.get(indexPk);
            if (valuePk != null) {
                pkToRecord.put(valuePk, record);
            }
        }
        return super.add(record);
    }

    /**
     * Вставка данных если из сервера то fields[0] = SELECT если из клиента то
     * fields[0] = INSERT
     */
    public void add(int index, Record record) {
        //Record record = isAl(elements);
        if (indexPk != null) {
            Object valuePk = record.get(indexPk);
            if (valuePk != null) {
                pkToRecord.put(valuePk, record);
            }
        }
        super.add(index, record);
    }

// <editor-fold defaultstate="collapsed">
    /*private Record isAl(Record elements) {
        Record record = (elements == null) ? new Record(this) : elements;
        if (elements == null) {
            for (Field field : fields) {
                record.add(field.value());
            }
        }
        return record;
    }*/
// <editor-fold>    
    /**
     * Вставка данных fields[0] = UPDATE
     */
    public void set(Object value, int index, Field field) {
        Object v = get(index, field);
        if (v != null && value != null && v.equals(value)) {
            return;
        }
        ArrayList rowTable = super.get(index);
        rowTable.set(field.ordinal(), value);
    }

    /**
     * Возвращает значение поля field
     */
    public Object get(int index, Field field) {
        if (index == -1) {
            return null;
        }
        Record rowTable = super.get(index);
        if (rowTable == null) {
            return null;
        }
        return rowTable.get(field);
    }

    /**
     * Возвращает значение поля field
     */
    public Integer getInt(int index, Field field) {
        if (index == -1) {
            return null;
        }
        Record rowTable = super.get(index);
        if (rowTable == null) {
            return null;
        }
        return rowTable.getInt(field);
    }

    /**
     * Возвращает значение поля field
     */
    public String getStr(int index, Field field) {
        if (index == -1) {
            return null;
        }
        Record rowTable = super.get(index);
        if (rowTable == null) {
            return null;
        }
        return rowTable.getStr(field);
    }

    /**
     * Возвращает значение поля field или def
     */
    public Object get(int index, Field field, Object def) {
        Object obj = get(index, field);
        return obj == null ? def : obj;
    }

    /**
     * Возвращает значение поля field или def
     */
    public <T> T getAs(int index, Field field, Object def) {
        Object obj = get(index, field);
        return (obj == null) ? (T) def : (T) obj;
    }

    /**
     * Возвращает значение поля field или def
     */
    public <T> T getAs(ArrayList record, Field field, Object def) {
        Object obj = record.get(field.ordinal());
        Double lll = (Double) obj;
        Object val = (obj == null) ? (T) def : (T) obj;
        return (obj == null) ? (T) def : (T) obj;
    }

// <editor-fold defaultstate="collapsed"> 
    /*public Object getAt(int index, Field field) {
        Record record = get(index);
        return record.getAt(field);
    }

    public Object getAt(int index, Field field, Object def) {
        Object obj = getAt(index, field);
        return obj == null ? def : obj;
    }*/
// </editor-fold>         
    /**
     * Возвращает HashMap<Integer, T>
     */
    public <T> HashMap<Integer, T> mapAdded(Field key, String value) {
        if (key == null) {
            return null;
        }
        HashMap<Integer, T> hm = new HashMap();
        Iterator itr = iterator();
        while (itr.hasNext()) {

            Record record = (Record) itr.next();
            if (value == null) {
                hm.put(record.getInt(key), (T) record);
            } else {
                hm.put(record.getInt(key), (T) record.get(value));
            }
        }
        return hm;
    }

    /**
     * Получение пустой записи
     */
    public Record newRecord() {
        int indexMax = 0;
        for (Field field : fields) {
            indexMax = (field.ordinal() > indexMax) ? field.ordinal() : indexMax;
        }
        Record record = new Record(this);
        while (indexMax-- >= 0) {
            record.add(null);
        }
        return record;
    }

    /**
     * Возвращает Record по ключу используя TreeMap
     */
    public Record mapRecord(Object elementPk) {
        if (elementPk != null) {
            //if (elementPk instanceof Integer) {
            return pkToRecord.get(elementPk);
        }
        return null;
    }

    /**
     * Ищем индекс объекта DataRecord в таблице DataTable
     */
    public int getIndex(Record record) {
        //цикл по записям в таблице
        for (int indexRow = 0; indexRow < size(); ++indexRow) {
            if (this.get(indexRow).equals(record)) {
                return indexRow;
            }
        }
        return -1;
    }
}
