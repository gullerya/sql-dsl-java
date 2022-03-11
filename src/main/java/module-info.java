module com.gullerya.sqldsl {
	requires java.persistence;
	requires java.sql;
	requires org.slf4j;

	exports com.gullerya.sqldsl;
	exports com.gullerya.sqldsl.api.clauses;
}
