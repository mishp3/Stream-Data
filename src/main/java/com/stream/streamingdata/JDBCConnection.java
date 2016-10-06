package com.stream.streamingdata;

/**
 *  @author mishp3
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnection {

	private static Connection conn;
	static final String url = "jdbc:postgresql://10.61.9.168:5432/pega";
	static String user = null;
	static String password = null;

	public static Connection getConnection() {
		if (user == null)
			user = "pega";
		if (password == null)
			password = "pega";
		try {
			Class.forName("org.postgresql.Driver");
			conn = (Connection) DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
}
