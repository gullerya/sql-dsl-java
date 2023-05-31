package com.gullerya.restdsl.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Stream;

public class Filter {
	public final FilterOperator operator;
	public final List<Filter> operands;
	public final String field;
	public final List<String> values;

	public Filter(String input) {
		if (input == null || input.isEmpty()) {
			throw new IllegalArgumentException("input is null or empty");
		}

		List<Object> tokens = tokenize(input.toCharArray());

		operator = (FilterOperator) tokens.get(0);
		if (operator.composite) {
			operands = null;
			field = null;
			values = null;
		} else {
			operands = null;
			field = ((StringBuilder) tokens.get(1)).toString();
			values = tokens
					.subList(2, tokens.size())
					.stream()
					.map(StringBuilder.class::cast)
					.map(StringBuilder::toString)
					.toList();
		}
	}

	private List<Object> tokenize(char[] input) {
		Stack<List<Object>> nesting = new Stack<>();
		List<Object> currentList = new ArrayList<>();
		nesting.push(currentList);

		FilterOperator currentOp = null;
		StringBuilder currentToken = new StringBuilder();
		char currentChar;
		int currentIndex = 0;
		while (currentIndex < input.length) {
			currentChar = input[currentIndex];
			switch (currentChar) {
				case '(' -> {
					currentOp = FilterOperator.fromToken(currentToken.toString());
					if (currentOp == null) {
						throw new IllegalArgumentException("invalid operator '" + currentToken + "'");
					}
					currentList.add(currentOp);
					if (currentOp.composite) {
						List<Object> nestedList = new ArrayList<>();
						currentList.add(nestedList);
						nesting.push(currentList);
						currentList = nestedList;
					}
					currentToken = new StringBuilder();
				}
				case ')' -> {
					if (!currentToken.isEmpty()) {
						currentList.add(currentToken);
					}
					if (currentOp != null && currentOp.composite) {
						currentList = nesting.pop();
						currentOp = (FilterOperator) currentList.get(0);
					}
					currentToken = new StringBuilder();
				}
				case ',' -> {
					if (!currentToken.isEmpty()) {
						currentList.add(currentToken);
						currentToken = new StringBuilder();
					}
				}
				default -> currentToken.append(currentChar);
			}
			currentIndex++;
		}
		return nesting.pop();
	}
}
