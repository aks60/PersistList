package com.aks.dataset;

import java.util.HashSet;

/**
 * 
 * @author Aksenov Sergey
 *
 * <p>
 * Exception code name </p>
 */
public enum eExcep {

    CONSOL("На консоль"),
    FRAME("В диалоговое окно"),
    LOGFILE("В лог. файл"),
    //
    yesConn("Соединение успешно установлено"),
    findDrive("Не найден файл драйвера"),
    loadDrive("Ошибка загрузки файла драйвера"),
    noLogin("Ошибка ввода имени пользовоавтеля или пароля", 335544472, 1045, 18456),
    noGrant("Отсутствие прав(роли) доступа к базе данных", 335544352, 1044, 229),
    noBase("Не найдена база данных", 335544344, 1049, 8003),
    noPort("Порт закрыт или занят другой программой"),
    noConn("Ошибка соединения с базой данных"),
    noTable("Не найдена таблица в базе данных", 335544569, 1146);
    public int id = 0;
    public String mes;
    private HashSet<Integer> code = new HashSet();
    public static int countErr = 0;
    private static eExcep outputErr = FRAME;

    eExcep(String mes, int... codes) {
        this.mes = mes;
        for (int cod : codes) {
            this.code.add(cod);
        }
    }
}
