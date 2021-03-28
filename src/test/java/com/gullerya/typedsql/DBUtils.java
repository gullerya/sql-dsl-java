package com.gullerya.typedsql;

import com.gullerya.typedsql.configuration.DataSourceDetails;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtils {
	private static volatile boolean schemaInPlace = false;

	public static final String DAL_TESTS_SCHEMA = "DalTests";

	synchronized public static void ensureSchema(DataSourceDetails dsd) {
		if (!schemaInPlace) {
			if (dsd == null) {
				throw new IllegalArgumentException("data source details MUST NOT be NULL");
			}
			try (Connection c = dsd.getDataSource().getConnection(); Statement s = c.createStatement()) {
				System.out.println("preparing schema " + DAL_TESTS_SCHEMA + "...");
				s.execute("CREATE SCHEMA IF NOT EXISTS \"" + DAL_TESTS_SCHEMA + "\"");
				schemaInPlace = true;
				System.out.println("... schema " + DAL_TESTS_SCHEMA + " is ready");
			} catch (SQLException sqle) {
				throw new IllegalStateException("failed to prepare DB", sqle);
			}
		}
	}
}
