package com.gullerya.sqldsl.statements;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.OrderBy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertFewTest {
	private static final String SCHEMA = "InsertFewTestsSchema";
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
						+ "name VARCHAR(32), active BOOLEAN, b SMALLINT, s SMALLINT, "
						+ "children INT, l BIGINT, f FLOAT, height DECIMAL(15,4))")
				.execute();
	}

	@Test
	public void negativeNullCollection() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> {
					EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
					insertsDal.insert((List<TestPrivateA>) null);
				}
		);
	}

	@Test
	public void negativeEmptyCollection() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> {
					EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
					insertsDal.insert(new ArrayList<>());
				}
		);
	}

	@Test
	public void testPrivate() {
		int total = 10;
		List<TestPrivateA> few = new ArrayList<>();
		EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);

		Date d = Date.valueOf(LocalDate.now());
		for (int i = 0; i < total; i++) {
			TestPrivateA o = new TestPrivateA();
			o.firstName = "First " + i;
			o.lastName = "Last";
			o.birthDate = d;
			o.active = true;
			o.children = 3;
			o.height = 1.76F;
			few.add(o);
		}
		int[] r = insertsDal.insert(few);
		assertEquals(total, r.length);
		for (int or : r) assertEquals(1, or);

		List<TestPrivateA> tf = insertsDal
				.select("firstName", "lastName", "bdate", "active", "children", "height")
				.orderBy(OrderBy.asc("firstName"))
				.readAll();

		assertEquals(total, tf.size());
		for (int i = 0; i < total; i++) {
			TestPrivateA t = tf.get(i);
			assertEquals("First " + i, t.firstName);
			assertEquals("Last", t.lastName);
			assertEquals(d, t.birthDate);
			assertEquals(true, t.active);
			assertEquals(3, t.children);
			assertEquals(1.76, t.height, 0.01);
		}
	}

	@Test
	public void testPrimitives() {
		int total = 10;
		List<TestPrimitivesA> few = new ArrayList<>();
		EntityDAL<TestPrimitivesA> insertsDal = EntityDAL.of(TestPrimitivesA.class, dataSource);

		for (int i = 0; i < total; i++) {
			TestPrimitivesA o = new TestPrimitivesA();
			o.name = "First " + i;
			o.active = true;
			o.b = 1;
			o.s = 2;
			o.children = 3;
			o.l = 4;
			o.f = 2.23F;
			o.height = 1.76;
			few.add(o);
		}
		int[] r = insertsDal.insert(few);
		assertEquals(total, r.length);
		for (int or : r) assertEquals(1, or);

		List<TestPrimitivesA> tf = insertsDal
				.select("name", "active", "b", "s", "children", "l", "f", "height")
				.orderBy(OrderBy.asc("name"))
				.readAll();

		for (int i = 0; i < total; i++) {
			TestPrimitivesA t = tf.get(i);
			assertEquals("First " + i, t.name);
			assertTrue(t.active);
			assertEquals(1, t.b);
			assertEquals(2, t.s);
			assertEquals(3, t.children);
			assertEquals(4, t.l);
			assertEquals(2.23, t.f, 0.01);
			assertEquals(1.76, t.height, 0.01);
		}
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
		public String name;

		@Column
		public boolean active;

		@Column
		private byte b;

		@Column
		private short s;

		@Column
		private int children;

		@Column
		private long l;

		@Column
		private float f;

		@Column
		private double height;
	}
}
