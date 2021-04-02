package com.gullerya.sqldsl.api.clauses;

public interface GroupBy<S> {

	S groupBy(String... fields);

}
