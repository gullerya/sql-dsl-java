package com.gullerya.typedsql.entities;

import java.util.List;

public interface SelectTermAction<T> {

	/**
	 * read a single entity
	 * - if more than one entity resulted, IllegalStateException is thrown
	 *
	 * @return entity; MAY be NULL if none matched
	 */
	T readSingle();

	/**
	 * read all entities matching query, if any
	 * - neither offset nor limit applied
	 *
	 * @return list of entities; MAY be an EMPTY list; MAY NOT be NULL
	 */
	List<T> read();

	/**
	 * read all entities matching query, if any
	 * - no offset applied
	 *
	 * @param limit limit number; MUST be greater than 0
	 * @return list of entities; MAY be an EMPTY list; MAY NOT be NULL
	 */
	List<T> read(int limit);

	/**
	 * read all entities matching query, if any
	 *
	 * @param offset offset number; MUST be greater than 0
	 * @param limit  limit number; MUST be greater than 0
	 * @return list of entities; MAY be an EMPTY list; MAY NOT be NULL
	 */
	List<T> read(int offset, int limit);
}
