module com.gullerya.sqldsl {
	requires java.persistence;
	requires java.sql;

	exports com.gullerya.sqldsl;
	exports com.gullerya.sqldsl.api.clauses;
	exports com.gullerya.sqldsl.api.statements;
}
