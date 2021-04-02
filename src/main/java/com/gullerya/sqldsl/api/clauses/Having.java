package com.gullerya.sqldsl.api.clauses;

public interface Having<S> {

	S having(String... fields);

}
