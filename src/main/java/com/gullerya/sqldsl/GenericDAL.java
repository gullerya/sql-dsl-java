package com.gullerya.sqldsl;

import javax.sql.DataSource;

/**
 * API for Generic Data Access operations
 * - running several queries in transaction
 * - running compound queries:
 *   - SELECT out of nested SELECT/s
 *   - JOIN clauses
 */
public interface GenericDAL {

	/**
	 * Instantiate generic DAL for an operations like joins or select/s from select/s
	 *
	 * @param dataSource DataSource parameter to access the DB
	 * @return DAL instance
	 */
	static GenericDAL using(DataSource dataSource) {
		validateDataSource(dataSource);
		return null;
	}

	private static void validateDataSource(DataSource ds) {
		if (ds == null) {
			throw new IllegalArgumentException("data source MUST NOT be NULL");
		}
	}
}
