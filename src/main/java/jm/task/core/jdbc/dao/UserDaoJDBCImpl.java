package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS users (" +
            "id BIGINT PRIMARY KEY NOT NULL AUTO_INCREMENT" +
            ",name VARCHAR(45) NOT NULL" +
            ", lastName VARCHAR(45) NOT NULL" +
            ", age TINYINT NULL)";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS users";
    private static final String INSERT_USER = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";
    private static final String DELETE_USER = "DELETE FROM users WHERE id = ?";
    private static final String SELECT_ALL_USERS = "SELECT * FROM my_db.users";
    private static final String CLEAN_TABLE = "TRUNCATE users";

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
            try {
                connection.commit();
                System.out.println("Таблица пользователей успешно создана!");
            } catch (SQLException e) {
                System.out.println("Не удалось создать таблицу пользователей!");
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(DROP_TABLE);
            try {
                connection.commit();
                System.out.println("Таблица пользователей удалена!");
            } catch (SQLException e) {
                System.out.println("Не удалость удалить таблицу пользователей!");
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT_USER)) {
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.executeUpdate();
            try {
                connection.commit();
                System.out.println("Пользователь " + name + " " + lastName + " успешно добавлен!");
            } catch (SQLException e) {
                System.out.println("Не удалось добавить пользователя!");
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_USER)) {
            statement.setLong(1, id);
            statement.executeUpdate();
            try {
                connection.commit();
                System.out.println("Пользователь с id " + id + " удален!");
            } catch (SQLException e) {
                System.out.println("Не удалось удалить пользователя!");
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL_USERS);
             ResultSet resultSet = statement.executeQuery(SELECT_ALL_USERS)) {
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                userList.add(user);
                try {
                    connection.commit();
                    System.out.println("Cоздан список пользователей!");
                } catch (SQLException e) {
                    System.out.println("Не удалось получить список пользователей!");
                    connection.rollback();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userList;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(CLEAN_TABLE);
            try {
                connection.commit();
                System.out.println("Таблица очищена!");
            } catch (SQLException e) {
                System.out.println("Не удалось очистить таблицу!");
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
