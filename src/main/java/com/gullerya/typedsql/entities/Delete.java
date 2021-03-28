package com.gullerya.typedsql.entities;

interface Delete<T> {

	/**
	 * delete all records of the entity T
	 *
	 * @return number of affected rows
	 */
	int delete();

	/**
	 * delete records matching to the specified condition
	 *
	 * @param whereClause where clause; MUST NOT be NULL
	 * @return number of affected rows
	 */
	int delete(Where.WhereClause whereClause);
}
