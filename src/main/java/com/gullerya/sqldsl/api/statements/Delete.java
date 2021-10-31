package com.gullerya.sqldsl.api.statements;

import com.gullerya.sqldsl.api.clauses.Where;

public interface Delete<T> {

	/**
	 * delete all records of the entity T
	 *
	 * @return number of affected rows
	 */
	int deleteAll();

	/**
	 * delete records matching to the specified condition
	 *
	 * @param whereClause where clause; MUST NOT be NULL
	 * @return number of affected rows
	 */
	int delete(Where.WhereClause whereClause);
}
