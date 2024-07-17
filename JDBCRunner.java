import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "GuitarStores";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            // Получение всех таблиц базы данных
            getStores(connection);
            System.out.println();
            getGuitars(connection);
            System.out.println();
            getBuys(connection);
            System.out.println();

            // Запросы на получение данных
            getFullNameAndAgeBySerialNumber(connection, 5);
            System.out.println();
            getGuitarsByPrice(connection);
            System.out.println();

            // Запросы на добавление данных
            addGuitar(connection, "Россия", 60000, "Сосна");
            System.out.println();
            addStore(connection, "Москва", 3000000, 600);
            System.out.println();

            // Запросы на удаление данных
            removeBuyByIdClient(connection, 12);
            System.out.println();
            removeBuyByIdStore(connection, 1);
            System.out.println();

            // Запросы на обновление данных
            correctPriceGuitarBySerialNumber(connection,50000,4);
            System.out.println();
            correctMonthlyProfitByIdStore(connection,3500000,4);
            System.out.println();

            // Запросы на получениие данных из разных таблиц (JOIN)
            getClientsWithCityStore(connection);
            System.out.println();
            getClientsWithPriceGuitar(connection,18);
            System.out.println();


        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // Проверка подключения драйвера JDBC
    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    // Проверка подключения базы данных (GuitarStores)
    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
            connection.close();
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }



    // SELECT-запросы без параметров в одной таблице

    private static void getStores(Connection connection) throws SQLException {

        // значения ячеек
        int param0 = -1, param2 = -1, param3 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM \"Stores\" ORDER BY \"ID_Store\";"); // выполняем запроса на поиск и получаем список ответов

        System.out.println("ID_Store | City | MonthlyProfit | CustomerTurnover");
        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param0 = rs.getInt(1); // значение ячейки, можно получить по имени или по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            param2 = rs.getInt(3);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            param3 = rs.getInt(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }

    }

    static void getGuitars(Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null, param3 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM \"Guitars\" ORDER BY \"SerialNumber\";");  // выполняем запроса на поиск и получаем список ответов

        System.out.println("SerialNumber | CountryProduction | Price | Material");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно получить по имени или по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }

    }

    static void getBuys(Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1, param3 = -1, param4 = -1, param5 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM \"Buys\" ORDER BY \"ID_Client\";");   // выполняем запроса на поиск и получаем список ответов

        System.out.println("ID_Client | Name | Lastname | Age | SerialNumber_fk | ID_Store_fk");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | "
                    + param3 + " | " + param4 + " | " + param5);
        }

    }

    private static void getFullNameAndAgeBySerialNumber(Connection connection, int serialNumber) throws SQLException {
        if (serialNumber < 0) return; // Проверка на дурака

        long time = System.currentTimeMillis(); // Время начала выполнения запроса, нужно для того,чтобы посчитать за сколько времени выполнится этот запрос с выводом данных
        PreparedStatement statement = connection.prepareStatement("SELECT \"Name\" || ' ' || \"Lastname\" AS \"FullName\", \"Age\" " +
                "FROM \"Buys\" " +
                "WHERE \"SerialNumber_fk\" = ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, serialNumber); // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)

        ResultSet rs = statement.executeQuery();// выполняем запрос на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getInt(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)"); // Вывод потраченного времени
    }

    private static void getGuitarsByPrice(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        Statement statement = connection.createStatement();

        ResultSet rs = statement.executeQuery("SELECT * FROM \"Guitars\" " +
                "WHERE \"Price\" > 32000 ORDER BY \"Price\";");

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2)
                    + " | " + rs.getInt(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void addGuitar(Connection connection,
                                  String countryProduction, int price, String material) throws SQLException {

        if (countryProduction == null || countryProduction.isBlank()
                || material == null || material.isBlank() || price < 0) return; // Проверка на дурака

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO \"Guitars\"(\"CountryProduction\", " +
                        "\"Price\", \"Material\") VALUES (?, ?, ?) returning \"SerialNumber\";",
                Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?

        statement.setString(1, countryProduction); // "безопасное" добавление параметров
        statement.setInt(2, price);
        statement.setString(3, material);

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор гитары " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " guitar");
        getGuitars(connection);
    }

    private static void addStore(Connection connection,
                                 String city, int monthlyProfit, int customerTurnover) throws SQLException {

        if (city == null || city.isBlank() || monthlyProfit < 0 || customerTurnover < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO \"Stores\"(\"City\", " +
                        "\"MonthlyProfit\", \"CustomerTurnover\") VALUES (?, ?, ?) returning \"ID_Store\";",
                Statement.RETURN_GENERATED_KEYS);

        statement.setString(1, city);
        statement.setInt(2, monthlyProfit);
        statement.setInt(3, customerTurnover);

        int count =
                statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор магазина " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " store");
        getStores(connection);
    }

    private static void removeBuyByIdClient(Connection connection, int id) throws SQLException {

        if (id < 0) return; // Проверка на дурака

        PreparedStatement statement = connection.prepareStatement("DELETE from \"Buys\" " +
                "WHERE \"ID_Client\"= ?;"); // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?

        statement.setInt(1, id);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " buys");
        getBuys(connection);
    }


    private static void removeBuyByIdStore(Connection connection, int idStore) throws SQLException {
        if (idStore < 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from \"Buys\" WHERE \"ID_Store_fk\"= ?;");
        statement.setInt(1, idStore);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " buys");
        getBuys(connection);
    }

    private static void correctPriceGuitarBySerialNumber(Connection connection, int price, int serialNumber) throws SQLException {

        if (price < 0 || serialNumber < 0) return; // Проверка на дурака

        PreparedStatement statement = connection.prepareStatement("UPDATE \"Guitars\" SET \"Price\"=? WHERE \"SerialNumber\"=?;");
        statement.setInt(1, price); // сначала то, что передаем
        statement.setInt(2, serialNumber);   // затем то, по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " guitars");
        getGuitars(connection);
    }

    private static void correctMonthlyProfitByIdStore(Connection connection, int monthlyProfit, int idStore) throws SQLException {

        if (monthlyProfit < 0 || idStore < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE \"Stores\" SET \"MonthlyProfit\"=? WHERE \"ID_Store\"=?;");
        statement.setInt(1, monthlyProfit);
        statement.setInt(2, idStore);

        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " stores");
        getStores(connection);
    }

    private static void getClientsWithCityStore(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        Statement statement = connection.createStatement();   // создаем оператор для простого запроса (без параметров)

        ResultSet rs = statement.executeQuery("SELECT \"Name\", \"Lastname\", \"Age\", \"City\" " +
                "FROM \"Buys\" " +
                "JOIN \"Stores\" ON \"ID_Store_fk\" = \"ID_Store\";");    // выполняем запрос на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2)
                    + " | " + rs.getInt(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getClientsWithPriceGuitar(Connection connection, int age) throws SQLException {

        if (age < 0) return;

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT \"Name\", \"Age\", \"Price\" " +
                        "FROM \"Buys\" " +
                        "JOIN \"Guitars\" ON \"SerialNumber_fk\" = \"SerialNumber\"" +
                        "WHERE \"Age\" < ?;");
        statement.setInt(1, age);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getInt(3) );
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }


}