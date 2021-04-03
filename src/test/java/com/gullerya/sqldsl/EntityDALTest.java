package com.gullerya.sqldsl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EntityDALTest {
	private static final String SCHEMA = "EntityDalTestsSchema";
	private static final DataSource dataSource = DBUtils.getDataSource(SCHEMA);

	@Test
	public void testNegativeAllNulls() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(null, null)
		);
	}

	@Test
	public void testNegativeDSNull() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEntityA.class, null)
		);
	}

	@Test
	public void testNegativeEntityNotPublic() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEntityNotPublic.class, null)
		);
	}

	@Test
	public void testNegativeNotAnnotated() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEntityNotAnnotated.class, null)
		);
	}

	@Test
	public void testNegativeCTorNotPublic() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEntityCTorNotPublic.class, null)
		);
	}

	@Test
	public void testNegativeCTorHasParams() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEntityCTorHasParams.class, null)
		);
	}

	@Test
	public void testNegativeNoColumns() {
		Assertions.assertThrows(
				IllegalStateException.class,
				() -> EntityDAL.of(TestEntityNoColumns.class, dataSource)
		);
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

		@Override
		public String toString() {
			return "TestEntityA{" +
					"firstName='" + firstName + '\'' +
					", lastName='" + lastName + '\'' +
					", birthDate=" + birthDate +
					", active=" + active +
					", children=" + children +
					", height=" + height +
					'}';
		}
	}
}
