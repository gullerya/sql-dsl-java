package com.gullerya.sqldsl;

import java.time.LocalDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class EntityDALTest {
	private static final String SCHEMA = "EntityDalTestsSchema";
	private static final DataSource dataSource = DBUtils.getDataSource(SCHEMA);

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeAllNulls() {
		EntityDAL.of(null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeDSNull() {
		EntityDAL.of(TestEntityA.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeEntityNotPublic() {
		EntityDAL.of(TestEntityNotPublic.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeNotAnnotated() {
		EntityDAL.of(TestEntityNotAnnotated.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeCTorNotPublic() {
		EntityDAL.of(TestEntityCTorNotPublic.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeCTorHasParams() {
		EntityDAL.of(TestEntityCTorHasParams.class, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testNegativeNoColumns() {
		EntityDAL.of(TestEntityNoColumns.class, dataSource);
	}

	@Test
	public void testBase() {
		EntityDAL<TestEntityA> usersDal = EntityDAL.of(TestEntityA.class, dataSource);
		assertNotNull(usersDal);
	}

	static class TestEntityNotPublic {
	}

	public static class TestEntityNotAnnotated {
	}

	@Entity
	public static class TestEntityCTorNotPublic {
		TestEntityCTorNotPublic() {
		}
	}

	@Entity
	public static class TestEntityCTorHasParams {
		public TestEntityCTorHasParams(int p) {
			System.out.println(p);
		}
	}

	@Entity
	public static class TestEntityNoColumns {
	}

	@Entity
	@Table(schema = SCHEMA)
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
