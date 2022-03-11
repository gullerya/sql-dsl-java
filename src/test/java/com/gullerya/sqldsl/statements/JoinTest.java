package com.gullerya.sqldsl.statements;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.Join;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.*;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.*;


public class JoinTest {
	private static final String TABLE_NAME_A = "JoinTestTableA";
	private static final String TABLE_NAME_B = "JoinTestTableB";
	private static final String TABLE_NAME_C = "JoinTestTableC";
	private static final DataSource dataSource = DBUtils.getDataSource();

	@BeforeAll
	public static void prepare() throws Exception {
		for (String tableName : Arrays.asList(TABLE_NAME_A, TABLE_NAME_B, TABLE_NAME_C)) {
			dataSource.getConnection()
					.prepareStatement(
							"DROP TABLE IF EXISTS \"" + tableName + "\";" +
									"CREATE TABLE \"" + tableName + "\" (" +
									"id         BIGINT," +
									"state      INT," +
									"ready      BOOLEAN," +
									"first_name VARCHAR(128)," +
									"last_name  VARCHAR(128)," +
									"birthday   DATE," +
									"created    TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', CURRENT_TIMESTAMP)," +
									"updated    TIMESTAMP," +
									"locales    VARCHAR(128)" +
									");"
					)
					.execute();
		}
	}

	@Test
	public void basicE2EInner() {
		Join.inner();
	}

	@Test
	public void basicE2ETowards() {
		Join.toward();
	}

	@Entity
	@Table(name = TABLE_NAME_A)
	public static final class User {

		@Column(updatable = false, nullable = false)
		public long id;
		@Column(name = "first_name")
		public String firstName;
		@Column(name = "last_name")
		public String lastName;
		@Column
		public LocalDate birthday;

		public User() {
		}

		User(Long id, String firstName, String lastName, LocalDate birthday) {
			this.id = id;
			this.firstName = firstName;
			this.lastName = lastName;
			this.birthday = birthday;
		}
	}
}
