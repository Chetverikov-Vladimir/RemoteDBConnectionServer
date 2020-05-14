import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class DatabaseConnectNew {
    // Название классов для загрузки JDBC драйверов
    private static final String DRIVER_HIVE = "org.apache.hive.jdbc.HiveDriver";
    private static final String DRIVER_ORACLE = "oracle.jdbc.driver.OracleDriver";
    // Данные для подключения к базам
    private static final String URI_ORACLE = "jdbc:oracle:thin:@//mill.com:1521/toir";
    private static final String URI_HIVE = "jdbc:hive2://hhive01.com:10000/nlmk";
    private static final String USER_ORACLE = "usernameOracle";
    private static final String USER_HIVE = "usernameHive";
    private static final String PASSWORD_ORACLE = "passOracle";
    private static final String PASSWORD_HIVE = "passHive";

    //Отключение автокоммита в Оракл
    private static final boolean ORACLE_AUTOCOMMIT = false;

    //Глбальные переменные для передачи из Oracle
    private static int pointsCountGlobal; //Количество последовательных точек для преобразования Фурье
    private static String dateSelectGlobal; //Текущая дата
    private static int rollsArgsCount; //Количество параметров роликов за раз. 2 параметра на ролик

    //Глобальная переменная для возврата количества строк
    public static String returnRowCount;


    public static void main(String[] args) throws SQLException {

//Задаем параметры, переданные из клиента
        dateSelectGlobal=args[0];
        pointsCountGlobal=Integer.parseInt(args[1]);
        rollsArgsCount=Integer.parseInt(args[2]);

//Загрузка драйверов для hive и oracle 12.2.0.1
        try {
            Class.forName(DRIVER_HIVE);
           // DriverManager.registerDriver(new org.apache.hive.jdbc.HiveDriver());
           Class.forName(DRIVER_ORACLE);
          //  DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

        } catch (ClassNotFoundException e) {
            System.out.println("Не удалось загрузить драйвер. Библиотека драйвера JDBC не найдена.");
            e.printStackTrace();
            System.exit(1);
        }


//Получение коннектов к базам
        Connection hiveConnection = null;
        Connection oracleConnection = null;
        try {
            hiveConnection = DriverManager.getConnection(URI_HIVE, USER_HIVE, PASSWORD_HIVE);
            oracleConnection = DriverManager.getConnection(URI_ORACLE, USER_ORACLE, PASSWORD_ORACLE);
        } catch (SQLException e) {
            System.out.println("Ошибка подключения к базам данных");
            e.printStackTrace();
        }


//Получение Statement в Oracle
        oracleConnection.setAutoCommit(ORACLE_AUTOCOMMIT);
        Statement oracleStatement = oracleConnection.createStatement();


//Начало выполнения программы
        long timestart = System.currentTimeMillis();


//Очищаем таблицу
        System.out.println("Подключение успешно! Очищаю таблицу ROLL.");
        String sqlTrunc = "truncate table roll";
        oracleStatement.executeQuery(sqlTrunc);
        System.out.println("Таблица очищена");



//Выбираем ролики из базы Oracle и кладем их в лист
        System.out.println("Получаем обозначения скоростей и токов роликов из таблицы FIELDS.");
        String sqlGetRolls = "select * from fields order by 1"; //fetch first 20 rows only   where alia= 'ПРИВОД M060'
        System.out.println("SQL Запрос на выгрузку: " + sqlGetRolls);
        ResultSet oracleGetRollsResult = oracleStatement.executeQuery(sqlGetRolls);
        List<String> rollsNames = new LinkedList<>();
        while (oracleGetRollsResult.next()) {
            rollsNames.add(oracleGetRollsResult.getString(1));
        }
        System.out.println("Выбрано " + rollsNames.size() + " параметров роликов (2 параметра на ролик).");


//Выгрузка партии роликов из Hive и инсерт в Oracle
        List<Roll> rolls = new ArrayList<>();
        int countRoll = 0;
        int rollNamesStart = rollsNames.size() / 2;
        while (rollsNames.size() > 0) {
            long startImport = System.currentTimeMillis();

//Начинаем выгрузку партии роликов из Hive
            System.out.println("Выгружаю из Hive партию роликов");
            rolls.addAll(selectRolls(rollsNames, hiveConnection));
            System.out.println("Партия выгружена за " + (System.currentTimeMillis() - startImport) / 1000 + " секунд. Начинаю загрузку партии в Oracle");

 //Начинаем загрузку партии в Oracle
            long startExport = System.currentTimeMillis();
            insertIntoOracle(rolls, oracleConnection);
            System.out.println("Партия загружена за " + (System.currentTimeMillis() - startExport) / 1000 + " секунд.");
            countRoll += rolls.size();
            System.out.println("Загружено " + countRoll + " из " + rollNamesStart + " роликов");
            System.out.println("Затрачено: " + (System.currentTimeMillis() - timestart) / 1000 + " секунд с начала работы");

//получаем количество загруженных строк на данном этапе
            String sql = "select count(*) from roll";
            ResultSet sqlRes = oracleStatement.executeQuery(sql);
            while (sqlRes.next()) {
                System.out.println("Добавлено: " + sqlRes.getLong(1) + " строк");
            }

            rolls.clear();
        }
        System.out.println("Всего затрачено: " + (System.currentTimeMillis() - timestart) / 1000 + " секунд");
        System.out.println("Дата выгрузки: "+new Date());
        //Освобождаем память
        rolls=null;

//Получаем текущее количество загруженных строк
        String sqlResultRowCount="select count(*) from roll";
        ResultSet sqlRes = oracleStatement.executeQuery(sqlResultRowCount);
        while (sqlRes.next()) {
            returnRowCount=String.valueOf(sqlRes.getLong(1));
        }

        //Закрываем соединения
        oracleConnection.close();
        hiveConnection.close();


    }

    private static void insertIntoOracle(List<Roll> rolls, Connection oracleConnection) throws SQLException {
        /*Подготовка запроса для загрузки роликов.
         */
        PreparedStatement sqlPreparedInsertOracle = oracleConnection.prepareStatement("insert into roll(ts,speed,curr,nam) Values(?,?,?,?)");
        int counter = 1;
        for (Roll roll : rolls) {
            System.out.println("Вставка ролика " + counter + " из " + rolls.size());
            counter++;
            int count = 0;
            int beginIndex = 0;
            int lastIndex = 0;
            for (int i = 0; i < roll.getTimestamps().size(); i++) {
                if (roll.getSpeeds().get(i) < 3 && roll.getSpeeds().get(i) > 1) {
                    if (count == 0) {
                        beginIndex = i;
                        count++;
                        continue;
                    }
                    if (count == pointsCountGlobal) {
                        lastIndex = beginIndex + pointsCountGlobal;
                        count = 0;
                        for (int j = beginIndex; j < lastIndex; j++) {
                            sqlPreparedInsertOracle.setTimestamp(1, roll.getTimestamps().get(j));
                            sqlPreparedInsertOracle.setDouble(2, roll.getSpeeds().get(j));
                            sqlPreparedInsertOracle.setDouble(3, roll.getAmperes().get(j));
                            sqlPreparedInsertOracle.setString(4, roll.getNames().get(j));
                            sqlPreparedInsertOracle.addBatch();

                        }
                        beginIndex = i;
                    }
                    count++;

                } else {
                    count = 0;
                }

            }

            sqlPreparedInsertOracle.executeBatch();
            oracleConnection.commit();
            //Очищаем загруженный ролик для освобождения памяти
            roll=null;
        }
        oracleConnection.commit();
        sqlPreparedInsertOracle.close();
    }

    private static List<Roll> selectRolls(List<String> rollsNames, Connection hiveConnection) throws SQLException {
     /*Подготовка запроса для выборки роликов.
        За один запрос выбирается не более ROLLS_COUNT параметров (ROLLS_COUNT/2 роликов)
      */

        int currentRollsCount = rollsNames.size() < rollsArgsCount ? rollsNames.size() / 2 : rollsArgsCount / 2;
        Statement hiveStatement = hiveConnection.createStatement();
        StringBuilder sqlGetResult = new StringBuilder("select ts ");

        for (int i = 0; i < currentRollsCount; i++) {
            String name = rollsNames.get(1);

            sqlGetResult.append(", `" + rollsNames.remove(0) + "`");
            sqlGetResult.append(", `" + rollsNames.remove(0) + "`");
            sqlGetResult.append(", \"" + name + "\"");

        }
        sqlGetResult.append(" FROM nlmk.te_trends_l2_p3h3m_cg_100u where yearmonthday=" + dateSelectGlobal + " order by 1"); //replace(date_sub(current_date,1),"-","")

        System.out.println("Запрос на выгрузку из Hive: " + sqlGetResult);

//Создается и заполняется партия роликов
        ResultSet res = hiveStatement.executeQuery(sqlGetResult.toString());
        List<Roll> rolls = new ArrayList<>();
        for (int i = 0; i < currentRollsCount; i++) {
            rolls.add(new Roll());
        }
        while (res.next()) {
            for (int i = 0; i < currentRollsCount; i++) {
                rolls.get(i).addValues(res.getTimestamp(1),
                        res.getDouble(i * 3 + 2),
                        res.getDouble(i * 3 + 3),
                        res.getString(i * 3 + 4));
            }
        }
        System.out.println("Партия из " + rolls.size() + " роликов выгружена");
        hiveStatement.close();
        return rolls;

    }

}





