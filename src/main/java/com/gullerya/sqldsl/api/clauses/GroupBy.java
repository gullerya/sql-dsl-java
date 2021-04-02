package com.gullerya.sqldsl.api.clauses;

public interface GroupBy<DS> {

	DS groupBy(String... fields);

}
