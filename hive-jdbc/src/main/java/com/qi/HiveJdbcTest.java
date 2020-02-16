package com.qi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcTest {

	private static final String DRIVER_CLASS="org.apache.hive.jdbc.HiveDriver";
	private static final String URL="jdbc:hive2://master:10000";
	private static final String USERNAME="root";
	private static final String PASSWORD="123456";
	
	private Connection connection;
	
	public void setConnection() throws ClassNotFoundException, SQLException {
		Class.forName(DRIVER_CLASS);
		connection=DriverManager.getConnection(URL,USERNAME,PASSWORD);
	}
	
	public void queryPoker() throws SQLException {
		Statement statement=connection.createStatement();
		String sql="select * from pokes";
		ResultSet resultSet=statement.executeQuery(sql);
		while (resultSet.next()) {
			System.out.println("foo:"+resultSet.getInt("foo")+",bar:"+resultSet.getString("bar"));
		}
	}
	
	public void createTable() throws SQLException {

		Statement statement=connection.createStatement();
		String sql="create table my_user (id int,name string,sex string) "
				+ "row format delimited "
				+ "FIELDS TERMINATED BY ' '";
		int executeUpdate = statement.executeUpdate(sql);
		System.out.println(executeUpdate);
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		HiveJdbcTest hiveJdbcTest=new HiveJdbcTest();
		hiveJdbcTest.setConnection();
//		hiveJdbcTest.queryPoker();
		hiveJdbcTest.createTable();
	}
	
}
