package jm.task.core.jdbc;

import jm.task.core.jdbc.dao.UserDao;
import jm.task.core.jdbc.dao.UserDaoHibernateImpl;
import jm.task.core.jdbc.dao.UserDaoJDBCImpl;


public class Main {
    public static void main(String[] args) {
        UserDao userDao = new UserDaoHibernateImpl();
        userDao.createUsersTable();
        userDao.saveUser("James", "Bond",(byte) 70);
        userDao.saveUser("Ostin", "Powers",(byte) 60);
        userDao.saveUser("Ronald", "McDonald",(byte) 115);
        userDao.saveUser("Piter", "Parker",(byte) 35);
        System.out.println(userDao.getAllUsers());
        userDao.removeUserById(2);
        System.out.println(userDao.getAllUsers());
        userDao.cleanUsersTable();
        System.out.println(userDao.getAllUsers());
        userDao.dropUsersTable();
    }
}
