package com.aks.dataset;

import com.aks.dataset.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author s.aksenov
 * <p>
 * Доступ к полям строк </p>
 */
public class Record<E> extends ArrayList<E> {

    public static boolean DIRTY = false;
    private Table table = null;
    private HashMap<String, Object> hmValue = new HashMap();

    public Record(Table table) {
        super();
        this.table = table;
    }

    public Record(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * ФУНКЦИЯ ИЗМЕНЕНИЯ СТАТУСА ЗАПИСЕЙ В БД!!! Эта функ. переопределена для
     * изменения статуса записи при редактировании
     */
    public E set(int index, E element) {
        //if(element != null) App.D("DataRecord.set " + element.toString());
        if (index != 0 && Query.SEL.equals(get(0))) {
            super.set(0, (E) Query.UPD);
            DIRTY = true;
        }
        return super.set(index, element);
    }

    /**
     * ФУНКЦИЯ ИЗМЕНЕНИЯ СТАТУСА ЗАПИСЕЙ В БД!!!
     */
    public E set(Field field, E element) {
        return (E) set(field.ordinal(), element);
    }

    /**
     * ЗАПИСЬ В РЕКОРД БЕЗ ИЗМЕНЕНИЯ СТАТУСА
     */
    public E setNo(int index, E element) {
        return super.set(index, element);
    }

    /**
     * ЗАПИСЬ В РЕКОРД БЕЗ ИЗМЕНЕНИЯ СТАТУСА
     */
    public E setNo(Field field, E element) {
        return super.set(field.ordinal(), element);
    }

    public Object get(Field field) {
        return super.get(field.ordinal());
    }

    public Object get(String name) {
        Integer i = (Integer) table.columnToIndex.get(name);
        return (i == null) ? null : super.get(i);
    }

    public String getStr(int index) {
        return (super.get(index) == null) ? "" : String.valueOf(super.get(index));
    }

    public int getInt(int index) {
        Object obj = super.get(index);
        return (obj == null) ? -1 : Integer.valueOf(String.valueOf(obj));
    }

    public float getFloat(int index) {
        Object obj = super.get(index);
        return (obj == null) ? -1 : Float.valueOf(String.valueOf(obj));
    }

    public double getDbl(int index) {
        Object obj = super.get(index);
        return (obj == null) ? -1 : Double.valueOf(String.valueOf(obj));
    }

    public Date getDate(int index) {
        return (super.get(index) == null) ? null : (Date) super.get(index);
    }

    public String getStr(Field field) {
        return (super.get(field.ordinal()) == null) ? "" : String.valueOf(super.get(field.ordinal()));
    }

    public int getInt(Field field) {
        try {
            Object obj = super.get(field.ordinal());
            return (obj == null) ? -1 : Integer.valueOf(String.valueOf(obj));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public float getFloat(Field field) {
        Object obj = super.get(field.ordinal());
        return (obj == null) ? -1 : Float.valueOf(String.valueOf(obj));
    }

    public double getDbl(Field field) {
        Object obj = super.get(field.ordinal());
        return (obj == null) ? -1 : Double.valueOf(String.valueOf(obj));
    }

    public Date getDate(Field field) {
        return (super.get(field.ordinal()) == null) ? null : (Date) super.get(field.ordinal());
    }

    /**
     * ДОБАВЛЕНИЕ ЗАПИСИ В РЕКОРД
     */
    public Object add(String field, Object element) {
        return hmValue.put(field, element);
    }

    /**
     * Возвращает ссылочное значение поля таблицы (value) применяется как
     * правило в DataModel2, ReportToHtml и т.д.
     */
    /*public Object getAt(Field field) {
        Object val = get(field.ordinal());
        return getAt(field, val);
    }
    
    public Object getAt(Field field, Object val) {
        if (val == null) {
            return null;
        }
        if (field.meta().type().equals(Field.TYPE.INTsp)) {
            int rowDict = eDict2.query().locate(eDict2.sp.meta().push(val));
            return eDict2.query().get(rowDict, eDict2.cname, "");
            
        } else if (field.meta().type().equals(Field.TYPE.INTfp)) {
            Field fieldFk = field.meta().foreignkey();
            Field fieldPk = fieldFk.fields()[1];
            int indexTable = fieldPk.table().locate(fieldPk.meta().push(val));
            return fieldFk.table().get(indexTable, fieldFk, "");
        } else {
            return val;
        }
    }*/
    /**
     * Проверка на корректность ввода
     */
    public String validate(Field... fields) {
        for (int index = 1; index < fields.length; index++) {
            MetaField prop = fields[index].meta();
            Object value = super.get(fields[index].ordinal());
            Object mes = prop.validate(value);
            if (mes != null) {
                return mes.toString();
            }
        }
        return null;
    }
}
