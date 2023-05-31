package com.gullerya.restdsl;

import com.gullerya.restdsl.filter.Filter;
import org.junit.jupiter.api.Test;

public class FilterTest {
	private static final String[] useCases = new String[]{
			"eq(f,v)",
			"ne(f,v)",
			"and(eq(f,v))",
			"and(eq(f,v),ne(f,v))"
	};

	@Test
	public void testFilters() {
		for (String useCase : useCases) {
			Filter f = new Filter(useCase);
		}
	}
}
