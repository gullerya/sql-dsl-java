package com.gullerya.restdsl.filter;

public enum FilterOperator {
	AND("and", true),
	OR("or", true),
	NOT("not", true),
	IN("in", false),
	EQUAL("eq", false),
	NOT_EQUAL("ne", false),
	LIKE("lk", false),
	NOT_LIKE("nl", false),
	GREATER("gt", false),
	GREATER_OR_EQUAL("ge", false),
	LESSER("lt", false),
	LESSER_OR_EQUAL("le", false);

	private final String token;
	public final boolean composite;

	FilterOperator(String token, boolean composite) {
		this.token = token;
		this.composite = composite;
	}

	static FilterOperator fromToken(String input) {
		for (FilterOperator fo : FilterOperator.values()) {
			if (fo.token.equals(input)) {
				return fo;
			}
		}
		return null;
	}
}
