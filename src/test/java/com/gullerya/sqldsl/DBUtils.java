package com.gullerya.sqldsl;

import java.sql.Connection;
import java.sql.Driver;
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

	private static DataSource dataSource;
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
		String dbPort = System.getenv("DB_PORT");
		if (dbHost == null) dbHost = "localhost";
		if (dbPort == null) dbPort = "5432";
		if (dataSource == null) {
			synchronized (DATASOURCE_LOCK) {
				if (dataSource == null) {
					HikariConfig config = new HikariConfig();
					config.setDriverClassName("org.postgresql.Driver");
					config.setJdbcUrl("jdbc:postgresql://" + dbHost + ":" + dbPort + "/sqldsltests");
					dataSource = new HikariDataSource(config);
				}
			}
		}
		return dataSource;
	}
}
