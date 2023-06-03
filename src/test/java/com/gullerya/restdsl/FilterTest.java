package com.gullerya.restdsl;

import com.gullerya.restdsl.filter.Filter;
import com.gullerya.restdsl.filter.FilterOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FilterTest {

	@Test
	public void testFiltersSanity() {
		String[] useCases = new String[]{
				"e(f,v)",
				"ne(f,v)",
				"and(e(f,v),ne(f,v))",
				"or(and(e(f1,v),ne(f2,v)),in(f3,v1,v2,v3))"
		};
		for (String useCase : useCases) {
			Filter f = new Filter(useCase);
			Assertions.assertNotNull(f);
		}
	}

	@Test
	public void testFiltersPlain0() {
		Filter f = new Filter("e(f,v)");
		assertPlainFilter(f, FilterOperator.EQUAL, "f", "v");
	}

	@Test
	public void testFiltersPlain1() {
		Filter f = new Filter("ne(f,v)");
		assertPlainFilter(f, FilterOperator.NOT_EQUAL, "f", "v");
	}

	@Test
	public void testFiltersComposite0() {
		Filter f = new Filter("and(e(f1,v1),ne(f2,v2))");
		Assertions.assertEquals(FilterOperator.AND, f.operator);
		Assertions.assertNull(f.field);
		Assertions.assertNull(f.values);
		Assertions.assertNotNull(f.operands);
		Assertions.assertEquals(2, f.operands.size());
		assertPlainFilter(f.operands.get(0), FilterOperator.EQUAL, "f1", "v1");
		assertPlainFilter(f.operands.get(1), FilterOperator.NOT_EQUAL, "f2", "v2");
	}

	@Test
	public void testFiltersComposite1() {
		Filter f = new Filter("or(and(in(f1,v1,v2,v3),ne(f2,v)),in(f3,v1,v2,v3))");
		Assertions.assertEquals(FilterOperator.OR, f.operator);
		Assertions.assertNull(f.field);
		Assertions.assertNull(f.values);
		Assertions.assertNotNull(f.operands);
		Assertions.assertEquals(2, f.operands.size());
		assertPlainFilter(f.operands.get(1), FilterOperator.IN, "f3", Arrays.asList("v1", "v2", "v3"));

		Assertions.assertNotNull(f.operands.get(0).operands);
		Assertions.assertEquals(2, f.operands.get(0).operands.size());
		assertPlainFilter(f.operands.get(0).operands.get(0), FilterOperator.IN, "f1", Arrays.asList("v1", "v2", "v3"));
		assertPlainFilter(f.operands.get(0).operands.get(1), FilterOperator.NOT_EQUAL, "f2", "v");
	}

	@Test
	public void testNegativeNullInput() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter(null)
		);
	}

	@Test
	public void testNegativeEmptyInput() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter("")
		);
	}

	@Test
	@Disabled
	public void testNegativeBadFormat0() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter("e(f,v")
		);
	}

	@Test
	@Disabled
	public void testNegativeBadFormat1() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter("and(e(f,v),ne(a,b)")
		);
	}

	@Test
	public void testNegativeBadFormat2() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter("e(f)")
		);
	}

	@Test
	public void testNegativeNonExisting() {
		Assertions.assertThrows(
				IllegalArgumentException.class, () -> new Filter("eq(f,v")
		);
	}

	private void assertPlainFilter(Filter filter, FilterOperator o, String f, List<String> v) {
		Assertions.assertEquals(o, filter.operator);
		Assertions.assertEquals(f, filter.field);
		Assertions.assertEquals(v, filter.values);
		Assertions.assertNull(filter.operands);
	}

	private void assertPlainFilter(Filter filter, FilterOperator o, String f, String v) {
		Assertions.assertEquals(o, filter.operator);
		Assertions.assertEquals(f, filter.field);
		Assertions.assertEquals(Collections.singletonList(v), filter.values);
		Assertions.assertNull(filter.operands);
	}
}
