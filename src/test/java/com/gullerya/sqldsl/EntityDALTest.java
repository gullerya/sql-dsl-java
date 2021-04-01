package com.gullerya.sqldsl;

import java.time.LocalDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import org.junit.Test;

public class EntityDALTest {
	private static final String SCHEMA = "EntityDalTestsSchema";
	private static final DataSource dataSource = DBUtils.getDataSource(SCHEMA);

	@Test
	public void testA() {
		EntityDAL<TestEntityA> usersDal = EntityDAL.of(TestEntityA.class, dataSource);
	}

	// TODO: add more tests, negative etc

	@Entity
	@Table(name = "", schema = SCHEMA)
	public static class TestEntityA {

		@Column(length = 10)
		private String firstName;

		@Column(length = 12)
		private String lastName;

		@Column(name = "bdate")
		private LocalDate birthDate;

		@Column(nullable = false)
		private Boolean active;

		@Column
		private int children;

		@Column
		private Float height;
	}
}
