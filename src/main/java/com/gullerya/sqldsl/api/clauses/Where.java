package com.gullerya.sqldsl.api.clauses;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.gullerya.sqldsl.impl.EntityMetaProc;

public interface Where<DS> {

	DS where(WhereClause where);

	/**
	 * clause validator
	 */
	static <T> void validate(EntityMetaProc<T> em, WhereClause where) {
		// if (where == null) {
		// throw new IllegalArgumentException("where clause MUST NOT be NULL");
		// }
		// Collection<String> fields = where.collectFields();
		// for (String f : fields) {
		// if (!em.byColumn.containsKey(f)) {
		// throw new IllegalArgumentException("field '" + f + "' not found in entity " +
		// em.type + " definition");
		// }
		// }
	}

	final class WhereFieldValuePair {
		public String column;
		public Object value;

		private WhereFieldValuePair(String column, Object value) {
			this.column = column;
			this.value = value;
		}
	}

	/**
	 * WHERE factory methods
	 */
	static WhereClause and(WhereClause one, WhereClause two, WhereClause... more) {
		List<WhereClause> tmp = Arrays.asList(one, two);
		tmp.addAll(Arrays.asList(more));
		return new WhereClause.CollectiveClause("AND", tmp);
	}

	static WhereClause and(Collection<WhereClause> clauses) {
		return new WhereClause.CollectiveClause("AND", clauses);
	}

	static WhereClause or(WhereClause one, WhereClause two, WhereClause... more) {
		List<WhereClause> tmp = new ArrayList<>();
		tmp.add(one);
		tmp.add(two);
		tmp.addAll(Arrays.asList(more));
		return new WhereClause.CollectiveClause("OR", tmp);
	}

	static WhereClause or(Collection<WhereClause> clauses) {
		return new WhereClause.CollectiveClause("OR", clauses);
	}

	static WhereClause not(WhereClause operand) {
		return new WhereClause.NotClause(operand);
	}

	static WhereClause isNull(String field) {
		return new WhereClause.IsNullClause(field);
	}

	static WhereClause isNotNull(String field) {
		return new WhereClause.IsNotNullClause(field);
	}

	static WhereClause eq(String field, Object value) {
		return new WhereClause(field, value, "=");
	}

	static WhereClause notEq(String field, Object value) {
		return new WhereClause(field, value, "<>");
	}

	static WhereClause lt(String field, Object value) {
		return new WhereClause(field, value, "<");
	}

	static WhereClause gt(String field, Object value) {
		return new WhereClause(field, value, ">");
	}

	static WhereClause lte(String field, Object value) {
		return new WhereClause(field, value, "<=");
	}

	static WhereClause gte(String field, Object value) {
		return new WhereClause(field, value, ">=");
	}

	static WhereClause like(String field, String pattern) {
		return new WhereClause(field, pattern, "LIKE");
	}

	static WhereClause in(String field, Object... values) {
		return new WhereClause.InClause(field, values);
	}

	static WhereClause in(String field, Collection<Object> values) {
		return new WhereClause.InClause(field, values);
	}

	static WhereClause between(String field, Object from, Object to) {
		return new WhereClause.BetweenClause(field, from, to);
	}

	/**
	 * Simple clause and base for complex cases
	 */
	class WhereClause {
		private final String column;
		private final Object value;
		private final String operator;

		private WhereClause(String column, Object value, String operator) {
			if (column == null || column.isEmpty()) {
				throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
			}
			if (value == null) {
				throw new IllegalArgumentException(
						"value MUST NOT be NULL (for NULL conditions use 'isNull' and 'isNotNull')");
			}
			this.column = column;
			this.value = value;
			this.operator = operator;
		}

		// for extension use only
		private WhereClause(String column) {
			if (column == null || column.isEmpty()) {
				throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
			}
			this.column = column;
			this.value = null;
			this.operator = null;
		}

		// for extension use only
		private WhereClause() {
			this.column = null;
			this.value = null;
			this.operator = null;
		}

		Collection<String> collectFields() {
			return collectFields(new ArrayList<>());
		}

		public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
			parametersCollection.add(new WhereFieldValuePair(this.column, this.value));
			return column + " " + operator + " ?";
		}

		Collection<String> collectFields(Collection<String> fc) {
			if (column != null) {
				fc.add(column);
			}
			return fc;
		}

		/**
		 * Collective clause AND/OR
		 */
		private static final class CollectiveClause extends WhereClause {
			private final String operator;
			private final Collection<WhereClause> clauses;

			private CollectiveClause(String operator, Collection<WhereClause> clauses) {
				if (clauses == null || clauses.size() < 2 || clauses.contains(null)) {
					throw new IllegalArgumentException("clauses MUST NOT be NULL nor LESS than 2");
				}
				this.operator = operator;
				this.clauses = Collections.unmodifiableCollection(clauses);
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				return "(" + clauses.stream().map(c -> c.stringify(parametersCollection))
						.collect(Collectors.joining(" " + operator + " ")) + ")";
			}

			@Override
			Collection<String> collectFields(Collection<String> fc) {
				for (WhereClause clause : clauses) {
					clause.collectFields(fc);
				}
				return fc;
			}
		}

		/**
		 * NOT
		 */
		private static final class NotClause extends WhereClause {
			private final WhereClause operand;

			private NotClause(WhereClause operand) {
				if (operand == null) {
					throw new IllegalArgumentException("operand MUST NOT be NULL");
				}
				this.operand = operand;
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				return "NOT " + operand.stringify(parametersCollection);
			}

			@Override
			Collection<String> collectFields(Collection<String> fieldsCollection) {
				return operand.collectFields(fieldsCollection);
			}
		}

		/**
		 * IS NULL
		 */
		private static final class IsNullClause extends WhereClause {
			private IsNullClause(String field) {
				super(field);
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				return super.column + " IS NULL";
			}
		}

		/**
		 * IS NOT NULL
		 */
		private static final class IsNotNullClause extends WhereClause {
			private IsNotNullClause(String field) {
				super(field);
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				return super.column + " IS NOT NULL";
			}
		}

		/**
		 * IN
		 */
		private static final class InClause extends WhereClause {
			private final Object[] values;

			private InClause(String field, Object... values) {
				super(field);
				if (values == null || values.length == 0) {
					throw new IllegalArgumentException("values MUST NOT be NULL nor EMPTY");
				}
				this.values = values;
			}

			private InClause(String field, Collection<Object> values) {
				super(field);
				if (values == null || values.isEmpty()) {
					throw new IllegalArgumentException("values MUST NOT be NULL nor EMPTY");
				}
				this.values = values.toArray(new Object[0]);
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				for (Object value : values) {
					parametersCollection.add(new WhereFieldValuePair(super.column, value));
				}
				return super.column + " IN (" + String.join(",", Collections.nCopies(values.length, "?")) + ")";
			}
		}

		/**
		 * BETWEEN
		 */
		private static final class BetweenClause extends WhereClause {
			private final Object from;
			private final Object to;

			private BetweenClause(String field, Object from, Object to) {
				super(field);
				if (from == null) {
					throw new IllegalArgumentException("lower bound value MUST NOT be NULL");
				}
				if (to == null) {
					throw new IllegalArgumentException("upper bound value MUST NOT be NULL");
				}
				this.from = from;
				this.to = to;
			}

			@Override
			public String stringify(Collection<WhereFieldValuePair> parametersCollection) {
				parametersCollection.add(new WhereFieldValuePair(super.column, from));
				parametersCollection.add(new WhereFieldValuePair(super.column, to));
				return super.column + " BETWEEN ? AND ?";
			}
		}
	}
}
