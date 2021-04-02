package com.gullerya.sqldsl.api.clauses;

public interface Having<DS> {

	DS having(String... fields);

}
