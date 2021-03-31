package com.gullerya.sqldsl.api.clauses;

import java.util.Set;

import com.gullerya.sqldsl.EntityDAL;

public interface OrderBy<DS> {

	DS orderBy(OrderByClause orderBy, OrderByClause... orderByMore);

	/**
	 * clause validator
	 */
	static <T> void validate(EntityDAL.EntityMetadata<T> em, Set<String> groupByFields,
			Set<OrderByClause> orderByFields) {
		// for (OrderByClause obc : orderByFields) {
		// if (!em.byColumn.containsKey(obc.field)) {
		// throw new IllegalArgumentException(
		// "field '" + obc.field + "' not found in entity " + em.type + " definition");
		// }
		// }
		// if (groupByFields != null && !groupByFields.isEmpty()) {
		// List<String> ill = new ArrayList<>();
		// for (OrderByClause obc : orderByFields) {
		// if (!groupByFields.contains(obc.field)) {
		// ill.add(obc.field);
		// }
		// }
		// if (!ill.isEmpty()) {
		// throw new IllegalArgumentException("field/s [" + String.join(", ", ill)
		// + "] is/are found in the ORDER BY clause, but NOT in the GROUP BY clause");
		// }
		// }
	}

	/**
	 * ORDER BY factory methods
	 */
	static OrderByClause asc(String field) {
		return new OrderByClause(field, "ASC");
	}

	static OrderByClause desc(String field) {
		return new OrderByClause(field, "DESC");
	}

	final class OrderByClause {
		public final String field;
		public final String direction;

		private OrderByClause(String field, String direction) {
			if (field == null || field.isEmpty()) {
				throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
			}
			this.field = field;
			this.direction = direction;
		}
	}
}
