package com.gullerya.sqldsl.api.clauses;

import com.gullerya.sqldsl.impl.EntityMetaProc;

public interface Having<DS> {

	DS having(String... fields);

	/**
	 * clause validator
	 */
	static <T> void validate(EntityMetaProc<T> em, String... fields) {
		// if (fields == null || fields.length == 0) {
		// throw new IllegalArgumentException("fields MUST NOT be NULL nor Empty");
		// }
		// for (String f : fields) {
		// if (!em.byColumn.containsKey(f)) {
		// throw new IllegalArgumentException("field '" + f + "' not found in entity " +
		// em.type + " definition");
		// }
		// }
	}
}
