package com.gullerya.restdsl.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Filter {
	private static final char DEFAULT_OPENER = '(';
	private static final char DEFAULT_CLOSER = ')';
	private static final char DEFAULT_SPLITTER = ',';

	public final FilterOperator operator;
	public final List<Filter> operands;
	public final String field;
	public final List<String> values;

	public Filter(String input) {
		this(new ParseContext(
				input == null ? null : input.toCharArray(),
				DEFAULT_OPENER,
				DEFAULT_CLOSER,
				DEFAULT_SPLITTER,
				0)
		);
	}

	private Filter(ParseContext parseContext) {
		if (parseContext.input == null || parseContext.input.length < 1) {
			throw new IllegalArgumentException("invalid input: " + (parseContext.input == null ? "null" : new String(parseContext.input)));
		}

		this.operator = parseOperator(parseContext);
		if (operator.composite) {
			field = null;
			values = null;
			operands = new ArrayList<>();
			char currentChar;
			while (parseContext.index < parseContext.input.length) {
				currentChar = parseContext.input[parseContext.index];
				if (currentChar == parseContext.closer) {
					parseContext.index++;
					break;
				} else if (currentChar == parseContext.splitter) {
					parseContext.index++;
				} else {
					Filter inner = new Filter(parseContext);
					operands.add(inner);
				}
			}
		} else {
			operands = null;
			List<String> tokens = parseTokens(parseContext, parseContext.splitter, parseContext.closer);
			if (tokens.size() < 2) {
				throw new IllegalArgumentException("too few values");
			}
			field = tokens.get(0);
			values = Collections.unmodifiableList(tokens.subList(1, tokens.size()));
		}
	}

	private static List<String> parseTokens(ParseContext parseContext, char splitter, char closer) {
		List<String> result = new ArrayList<>();
		StringBuilder token = new StringBuilder();
		int currentIndex = parseContext.index;
		char currentChar = parseContext.input[currentIndex];
		boolean closerFound = false;
		while (currentIndex < parseContext.input.length - 1) {
			if (currentChar == closer) {
				closerFound = true;
				break;
			} else if (currentChar == splitter) {
				result.add(token.toString());
				token = new StringBuilder();
			} else {
				token.append(currentChar);
			}
			currentIndex++;
			currentChar = parseContext.input[currentIndex];
		}
		parseContext.index = currentIndex + 1;
		result.add(token.toString());
		return result;
	}

	private static FilterOperator parseOperator(ParseContext parseContext) {
		List<String> tokens = parseTokens(parseContext, parseContext.splitter, parseContext.opener);
		FilterOperator result = FilterOperator.fromToken(tokens.get(0));
		if (result == null) {
			throw new IllegalArgumentException("invalid operator '" + tokens.get(0) + "'");
		}
		return result;
	}

	private static final class ParseContext {
		private final char[] input;
		private final char opener;
		private final char closer;
		private final char splitter;
		private int index;

		private ParseContext(
				char[] input,
				char opener,
				char closer,
				char splitter,
				int index
		) {
			this.input = input;
			this.opener = opener;
			this.closer = closer;
			this.splitter = splitter;
			this.index = index;
		}
	}
}
