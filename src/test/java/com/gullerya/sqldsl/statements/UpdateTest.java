package com.gullerya.sqldsl.statements;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.Literal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.*;
import javax.sql.DataSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.gullerya.sqldsl.api.clauses.OrderBy.asc;
import static com.gullerya.sqldsl.api.clauses.Where.*;
import static org.junit.jupiter.api.Assertions.*;

public class UpdateTest {
	private static final String TABLE_NAME = "UpdateTestTable";
	private static final DataSource dataSource = DBUtils.getDataSource();
	private static final EntityDAL<User> usersService = EntityDAL.of(User.class, dataSource);

	@BeforeAll
	public static void prepare() throws Exception {
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + TABLE_NAME + "\" (" +
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

	@Test
	public void testUpdateAll() {
		usersService.deleteAll();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(1000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(1001L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(1002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(1003L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(1004L, 1, true, "Sam", "5", bday));
		int[] result = usersService.insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		int updated = usersService
				.update(new User(null, 2, null, null, null, null))
				.all();
		assertEquals(5, updated);

		List<User> users = usersService.select(Collections.singleton("state")).readAll();

		assertNotNull(users);
		assertEquals(5, users.size());
		for (User user : users) {
			assertNotNull(user);
			assertEquals(2, (int) user.status);
		}
	}

	@Test
	public void testUpdateSome() {
		usersService.deleteAll();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(2000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(2001L, 1, false, "Sam", "2", bday));
		usersToInsert.add(new User(2002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(2003L, 1, false, "Sam", "4", bday));
		usersToInsert.add(new User(2004L, 1, true, "Sam", "5", bday));
		int[] result = usersService.insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		int updated = usersService
				.update(new User(null, 2, null, null, null, null))
				.where(or(Arrays.asList(eq("ready", false), and(eq("first_name", "Sam"), in("last_name", "1", "5")))));
		assertEquals(4, updated);

		List<User> users = usersService.select("id", "state").where(eq("state", 2)).orderBy(asc("id")).readAll();

		assertNotNull(users);
		assertEquals(4, users.size());
		for (User user : users) {
			assertNotNull(user);
			assertEquals(2, (int) user.status);
		}
		assertEquals(2000, (long) users.get(0).id);
		assertEquals(2001, (long) users.get(1).id);
		assertEquals(2003, (long) users.get(2).id);
		assertEquals(2004, (long) users.get(3).id);
	}

	@Test
	public void negativeA() {
		assertThrows(
				IllegalArgumentException.class,
				() -> usersService.update(null)
		);
	}

	@Test
	public void negativeB() {
		assertThrows(
				IllegalArgumentException.class,
				() -> usersService.update(new User())
		);
	}

	@Test
	public void negativeC1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					Literal[] ls = new Literal[0];
					usersService.update(new User(), ls);
				}
		);
	}

	@Test
	public void negativeC2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> usersService.update(new User(), (Literal[]) null)
		);
	}

	@Test
	public void negativeC3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> usersService.update(new User(), Literal.exp("none", "DEFAULT"))
		);
	}

	@Entity
	@Table(name = TABLE_NAME)
	public static final class User {
		@Column(updatable = false)
		public Long id;
		@Column(name = "state")
		public Integer status;
		@Column
		public Boolean ready;
		@Column(name = "first_name")
		public String firstName;
		@Column(name = "last_name")
		public String lastName;
		@Column
		public LocalDate birthday;
		@Column(updatable = false)
		public LocalDateTime created;
		@Column
		public LocalDateTime updated;
		@Column
		@Convert(converter = LocalesConverter.class)
		public String[] locales;

		public User() {
		}

		User(Long id, Integer status, Boolean ready, String firstName, String lastName, LocalDate birthday) {
			this.id = id;
			this.status = status;
			this.ready = ready;
			this.firstName = firstName;
			this.lastName = lastName;
			this.birthday = birthday;
		}

		public static final class LocalesConverter implements AttributeConverter<String[], String> {
			@Override
			public String convertToDatabaseColumn(String[] attribute) {
				return attribute == null ? null : String.join(",", attribute);
			}

			@Override
			public String[] convertToEntityAttribute(String dbData) {
				return dbData == null ? null : dbData.split(",");
			}
		}
	}
}
