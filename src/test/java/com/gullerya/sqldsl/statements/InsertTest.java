package com.gullerya.sqldsl.statements;

import java.sql.SQLException;
import java.time.LocalDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;

import org.junit.BeforeClass;
import org.junit.Test;

public class InsertTest {
	private static final String SCHEMA = "InsertTestsSchema";
	private static final String TEST_PRIVATE_TABLE = "TestPrivate";
	private static final DataSource dataSource = DBUtils.getDataSource(SCHEMA);

	@BeforeClass
	static public void before() throws SQLException {
		dataSource.getConnection()
				.prepareStatement("CREATE TABLE \"" + SCHEMA + "\".\"" + TEST_PRIVATE_TABLE + "\" ("
						+ "firstName VARCHAR(32), lastName VARCHAR(32), bdate DATE, active BOOLEAN, "
						+ "children INT, height DECIMAL(15,4))")
				.execute();
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA() {
		EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
		insertsDal.insert((TestPrivateA) null);
	}

	@Test
	public void testPrivate() {
		EntityDAL<TestPrivateA> insertsDal = EntityDAL.of(TestPrivateA.class, dataSource);
		TestPrivateA o = new TestPrivateA();
		o.firstName = "First";
		o.lastName = "Last";
		o.birthDate = LocalDate.now();
		o.active = true;
		o.children = 3;
		o.height = 1.76F;
		insertsDal.insert(o);
	}

	@Entity
	@Table(name = TEST_PRIVATE_TABLE, schema = SCHEMA)
	public static class TestPrivateA {

		@Column(length = 10)
		private String firstName;

		@Column(length = 12)
		private String lastName;

		@Column(name = "bdate")
		private LocalDate birthDate;

		@Column(nullable = false)
		private Boolean active;

		@Column
		private Integer children;

		@Column
		private Float height;
	}
}
