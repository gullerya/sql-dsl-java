package com.gullerya.sqldsl.api.statements;

import java.util.Collection;

import com.gullerya.sqldsl.Literal;

public interface Insert<T> {

	/**
	 * insert a single entity
	 * - NULL fields are skipped
	 * - literal MAY be used to set value to NULL
	 *
	 * @param entity   entity; MUST NOT be NULL
	 * @param literals literal values will override the corresponding field values in the entity
	 * @return true if the operation resulted in 1 affected row count, otherwise false
	 */
	boolean insert(T entity, Literal... literals);

	/**
	 * insert a collection of entities
	 * - NULL fields are skipped
	 * - literal MAY be used to set values to NULL
	 * - insert performed as batch
	 * - insert is uniform, meaning
	 * ---- if even one entity has a value in some field, this field will be set to NULL to any other entities that have NULL values
	 * ---- literal will be spread for all entities regardless of their field values
	 *
	 * @param entities entities; MUST NOT be NULL nor EMPTY
	 * @param literals literal values will override the corresponding field values in all the entities
	 * @return an array of insert result, correlated to the entities sent to insert
	 */
	int[] insert(Collection<T> entities, Literal... literals);
}
