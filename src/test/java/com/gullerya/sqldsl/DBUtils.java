package com.gullerya.sqldsl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Map;
import java.util.HashMap;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DBUtils {
	private static final Object DATASOURCE_LOCK = new Object();
	private static final Object SCHEMA_LOCK = new Object();

	private static volatile DataSource dataSource;
	private static final Map<String, Boolean> schemas = new HashMap<>();

	public static final String DEFAULT_TESTS_SCHEMA = "DalTests";

	public static DataSource getDataSource() {
		return getDataSource(DEFAULT_TESTS_SCHEMA);
	}

	public static DataSource getDataSource(String schema) {
		if (schema == null || schema.isEmpty()) {
			throw new IllegalArgumentException("schema parameter MUST NOT be NULL nor empty");
		}

		DataSource result = DBUtils.ensureDataSource();
		if (!schemas.containsKey(schema)) {
			synchronized (SCHEMA_LOCK) {
				if (!schemas.containsKey(schema)) {
					try (Connection c = result.getConnection(); Statement s = c.createStatement()) {
						System.out.println("preparing schema '" + schema + "'...");
						s.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
						s.execute("CREATE SCHEMA \"" + schema + "\"");
						schemas.put(schema, true);
						System.out.println("... schema '" + schema + "' is ready");
					} catch (SQLException sqle) {
						throw new IllegalStateException("failed to create scheme '" + schema + "'", sqle);
					}
				}
			}
		}
		return result;
	}

	private static DataSource ensureDataSource() {
		String dbHost = System.getenv("DB_HOST");
		dbHost = dbHost == null ? "localhost" : dbHost;

		String dbPort = System.getenv("DB_PORT");
		dbPort = dbPort == null ? "5432" : dbPort;

		String dbUser = System.getenv("DB_USER");
		dbUser = dbUser == null ? "postgres" : dbUser;

		String dbPass = System.getenv("DB_PASS");
		dbPass = dbPass == null ? "postgres" : dbPass;

		String dbDb = System.getenv("DB_DB");
		dbDb = dbDb == null ? "sqldsltests" : dbDb;

		if (dataSource == null) {
			synchronized (DATASOURCE_LOCK) {
				if (dataSource == null) {
					HikariConfig config = new HikariConfig();
					config.setDriverClassName("org.postgresql.Driver");
					config.setJdbcUrl("jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbDb);
//					config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//					config.setJdbcUrl("jdbc:sqlserver://localhost:1433;databaseName=" + dbDb);
					config.setUsername(dbUser);
					config.setPassword(dbPass);
					dataSource = new HikariDataSource(config);
				}
			}
		}
		return dataSource;
	}
}
