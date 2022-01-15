package com.gullerya.sqldsl.api.clauses;

public interface OrderBy<S> {

	S orderBy(OrderByClause orderBy, OrderByClause... orderByMore);

	record OrderByClause(String field, String direction) {
		public OrderByClause {
			if (field == null || field.isEmpty()) {
				throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
			}
		}

		@Override
		public String toString() {
			return field + " " + direction;
		}
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
}
