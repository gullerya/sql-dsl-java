package com.gullerya.sql;

public abstract class Literal {
	final String field;
	final String value;

	private Literal(String field, String value) {
		if (field == null || field.isEmpty()) {
			throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
		}
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException("value MUST NOT be NULL nor EMPTY");
		}
		this.field = field;
		this.value = value;
	}

	public static Literal exp(String field, String value) {
		return new LiteralExpression(field, value);
	}

	private static final class LiteralExpression extends Literal {

		private LiteralExpression(String field, String value) {
			super(field, value);
		}
	}
}
