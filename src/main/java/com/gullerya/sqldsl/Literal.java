package com.gullerya.sqldsl;

public abstract class Literal {
	public final String column;
	public final String value;

	private Literal(String column, String value) {
		if (column == null || column.isEmpty()) {
			throw new IllegalArgumentException("column MUST NOT be NULL nor EMPTY");
		}
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException("value MUST NOT be NULL nor EMPTY");
		}
		this.column = column;
		this.value = value;
	}

	public static Literal exp(String column, String value) {
		return new LiteralExpression(column, value);
	}

	private static final class LiteralExpression extends Literal {

		private LiteralExpression(String field, String value) {
			super(field, value);
		}
	}
}
