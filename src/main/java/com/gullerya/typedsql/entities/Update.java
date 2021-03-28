package com.gullerya.typedsql.entities;

interface Update<T> {

	/**
	 * uniform update of ONE or MANY entities (depending on the where clause)
	 * - updated values provided in the entity object
	 *
	 * @param updateSet entity serving as an update blueprint; MUST NOT be NULL
	 * @param literals  literal values will override the corresponding field values in the entity
	 * @return update flow downstream APIs
	 */
	UpdateDownstream update(T updateSet, Literal... literals);

	interface UpdateDownstream extends Where<Integer>, UpdateTermAction {
	}
}
