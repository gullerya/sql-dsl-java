package com.gullerya.sql;

import java.util.Set;

interface Select<T> {

	SelectDownstream<T> select(String... fields);

	SelectDownstream<T> select(Set<String> fields);

	interface SelectDownstream<T> extends Where<WhereDownstream<T>>, GroupBy<GroupByDownstream<T>>, Having<HavingDownstream<T>>, OrderBy<OrderByDownstream<T>>, SelectTermAction<T> {
	}

	interface WhereDownstream<T> extends GroupBy<GroupByDownstream<T>>, Having<HavingDownstream<T>>, OrderBy<OrderByDownstream<T>>, SelectTermAction<T> {
	}

	interface GroupByDownstream<T> extends Having<HavingDownstream<T>>, OrderBy<OrderByDownstream<T>>, SelectTermAction<T> {
	}

	interface HavingDownstream<T> extends OrderBy<OrderByDownstream<T>>, SelectTermAction<T> {
	}

	interface OrderByDownstream<T> extends SelectTermAction<T> {
	}
}
