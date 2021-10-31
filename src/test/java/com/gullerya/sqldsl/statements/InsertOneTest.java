package com.gullerya.sqldsl.statements;

import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;

import com.gullerya.sqldsl.api.clauses.Where;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertOneTest {
	private static final String SCHEMA = "InsertTestsSchema";
	private static final String TEST_PRIVATE_TABLE = "TestPrivate";
	private static final String TEST_PRIMITIVES_TABLE = "TestPrimitives";
	private static final DataSource dataSource = DBUtils.getDataSource(SCHEMA);

	@BeforeAll
	static public void before() throws SQLException {
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + SCHEMA + "\".\"" + TEST_PRIVATE_TABLE + "\";" +
								"CREATE TABLE \"" + SCHEMA + "\".\"" + TEST_PRIVATE_TABLE + "\" ("
								+ "firstName VARCHAR(32), lastName VARCHAR(32), bdate DATE, active BOOLEAN, "
								+ "children INT, height DECIMAL(15,4))")
				.execute();

		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + SCHEMA + "\".\"" + TEST_PRIMITIVES_TABLE + "\";" +
								"CREATE TABLE \"" + SCHEMA + "\".\"" + TEST_PRIMITIVES_TABLE + "\" ("
								+ "name VARCHAR(32), active BOOLEAN, "
								+ "children INT, height DECIMAL(15,4))")
				.execute();
	}

	@Test
	public void negativeNullEntity() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> {
					EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
					insertsDal.insert((TestPrivateA) null);
				}
		);
	}

	@Test
	public void testPrivate() {
		EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
		TestPrivateA o = new TestPrivateA();
		o.firstName = "First";
		o.lastName = "Last";
		o.birthDate = Date.valueOf(LocalDate.now());
		o.active = true;
		o.children = 3;
		o.height = 1.76F;
		insertsDal.insert(o);

		TestPrivateA t = insertsDal
				.select("firstName", "lastName", "bdate", "active", "children", "height")
				.where(Where.eq("firstName", "First"))
				.readOne();

		assertEquals(o.firstName, t.firstName);
		assertEquals(o.lastName, t.lastName);
		assertEquals(o.birthDate, t.birthDate);
		assertEquals(o.active, t.active);
		assertEquals(o.children, t.children);
		assertEquals(o.height, t.height, 0.01);
	}

	@Test
	public void testPrimitives() {
		EntityDAL<TestPrimitivesA> insertsDal = EntityDAL.of(TestPrimitivesA.class, dataSource);
		TestPrimitivesA o = new TestPrimitivesA();
		o.name = "First";
		o.active = true;
		o.children = 3;
		o.height = 1.76F;
		insertsDal.insert(o);

		TestPrimitivesA t = insertsDal
				.select("name", "active", "children", "height")
				.where(Where.eq("name", "First"))
				.readOne();

		assertEquals(o.name, t.name);
		assertEquals(o.active, t.active);
		assertEquals(o.children, t.children);
		assertEquals(o.height, t.height, 0.01);
	}

	@Entity
	@Table(name = TEST_PRIVATE_TABLE, schema = SCHEMA)
	public static class TestPrivateA {

		@Column(length = 10)
		private String firstName;

		@Column(length = 12)
		private String lastName;

		@Column(name = "bdate")
		private Date birthDate;

		@Column(nullable = false)
		private Boolean active;

		@Column
		private Integer children;

		@Column
		private Float height;
	}

	@Entity
	@Table(name = TEST_PRIMITIVES_TABLE, schema = SCHEMA)
	public static class TestPrimitivesA {

		@Column(length = 10)
		private String name;

		@Column
		private boolean active;

		@Column
		private int children;

		@Column
		private double height;
	}
}
