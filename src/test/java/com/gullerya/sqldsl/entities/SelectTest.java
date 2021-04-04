package com.gullerya.sqldsl.entities;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.OrderBy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.*;
import javax.sql.DataSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.gullerya.sqldsl.api.clauses.Where.*;
import static org.junit.jupiter.api.Assertions.*;

public class SelectTest {
	private static final String TABLE_NAME = "ReadTestTable";
	private static final DataSource dataSource = DBUtils.getDataSource();

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
	public void testReadAll() {
		EntityDAL.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(1000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(1001L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(1002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(1003L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(1004L, 1, true, "Sam", "5", bday));
		int[] result = EntityDAL.of(User.class, dataSource).insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityDAL.of(User.class, dataSource)
				.select("id", "state", "ready", "first_name", "birthday", "created")
				.read();

		assertNotNull(users);
		assertEquals(5, users.size());
		for (User user : users) {
			assertNotNull(user);
			assertNotEquals(0, user.id);
			assertEquals(1, user.status);
			assertTrue(user.ready);
			assertEquals(bday, user.birthday);
			assertNotNull(user.created);
		}
	}

	@Test
	public void testReadOne() {
		EntityDAL.of(User.class, dataSource).delete();

		User userToInsert = new User(2000L, 4, false, "Sam", "1", null);
		int ir = EntityDAL.of(User.class, dataSource).insert(userToInsert);
		assertEquals(1, ir);

		User user = EntityDAL.of(User.class, dataSource)
				.select("id", "state", "first_name", "birthday")
				.where(eq("id", 2000))
				.readSingle();

		assertNotNull(user);
		assertEquals(2000, user.id);
		assertEquals(4, user.status);
		assertEquals("Sam", user.firstName);
		assertNull(user.birthday);
	}

	@Test
	public void testReadManyNoLimit() {
		EntityDAL.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(3000L, 0, true, "Sam", "1", bday));
		usersToInsert.add(new User(3000L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(3000L, 0, true, "Sam", "3", bday));
		usersToInsert.add(new User(3001L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(3001L, 0, true, "Sam", "5", bday));
		int[] result = EntityDAL.of(User.class, dataSource).insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityDAL.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name")
				.where(and(eq("first_name", "Sam"), eq("state", 0)))
				.orderBy(OrderBy.desc("id"), OrderBy.asc("last_name"))
				.read();

		assertNotNull(users);
		assertEquals(3, users.size());
		for (User user : users) {
			assertNotNull(user);
			assertNotEquals(0, user.id);
			assertEquals("Sam", user.firstName);
			assertEquals(0, user.status);
			assertNotNull(user.lastName);
		}
		assertEquals(3001, users.get(0).id);
		assertEquals("5", users.get(0).lastName);
		assertEquals(3000, users.get(1).id);
		assertEquals("1", users.get(1).lastName);
		assertEquals(3000, users.get(2).id);
		assertEquals("3", users.get(2).lastName);
	}

	@Test
	public void testReadManyWithLimitNoOffset() {
		EntityDAL.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(4001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(4002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(4003L, 3, true, "ESam", "3", bday));
		usersToInsert.add(new User(4004L, 1, true, "DSam", "4", bday));
		usersToInsert.add(new User(4005L, 3, true, "CSam", "5", bday));
		usersToInsert.add(new User(4006L, 3, true, "BSam", "6", bday));
		usersToInsert.add(new User(4007L, 0, true, "ASam", "7", bday));
		int[] result = EntityDAL.of(User.class, dataSource).insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityDAL.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name", "birthday")
				.where(not(eq("state", 3)))
				.orderBy(OrderBy.asc("first_name"))
				.read(2);

		assertNotNull(users);
		assertEquals(2, users.size());
		assertEquals(4007, users.get(0).id);
		assertEquals(0, (long) users.get(0).status);
		assertEquals("ASam", users.get(0).firstName);
		assertEquals("7", users.get(0).lastName);
		assertEquals(bday, users.get(0).birthday);
		assertEquals(4004, users.get(1).id);
		assertEquals(1, (long) users.get(1).status);
		assertEquals("DSam", users.get(1).firstName);
		assertEquals("4", users.get(1).lastName);
		assertEquals(bday, users.get(1).birthday);
	}

	@Test
	public void testReadManyWithOffsetNoLimit() {
		EntityDAL.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		String[] lns = new String[]{"en", "he", "ru"};
		usersToInsert.add(new User(5001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(5002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(5003L, 3, true, "ESam", "3", bday));
		usersToInsert.add(new User(5004L, 1, true, "Jim", "4", bday));
		usersToInsert.add(new User(5005L, 3, false, "Jim", "5", bday));
		usersToInsert.add(new User(5006L, 3, false, "BSam", "6", bday));
		usersToInsert.add(new User(5007L, 0, false, "ASam", "7", bday));
		usersToInsert.forEach(u -> u.locales = lns);
		int[] result = EntityDAL.of(User.class, dataSource).insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityDAL.of(User.class, dataSource)
				.select("id", "ready", "first_name", "last_name", "birthday", "locales")
				.where(or(eq("ready", true), notEq("first_name", "Jim")))
				.orderBy(OrderBy.desc("last_name"))
				.read(2, Integer.MAX_VALUE);

		assertNotNull(users);
		assertEquals(4, users.size());
		assertEquals(5004, users.get(0).id);
		assertTrue(users.get(0).ready);
		assertEquals("Jim", users.get(0).firstName);
		assertEquals("4", users.get(0).lastName);
		assertEquals(bday, users.get(0).birthday);
		assertArrayEquals(lns, users.get(0).locales);

		assertEquals(5003, users.get(1).id);
		assertTrue(users.get(1).ready);
		assertEquals("ESam", users.get(1).firstName);
		assertEquals("3", users.get(1).lastName);
		assertEquals(bday, users.get(1).birthday);
		assertArrayEquals(lns, users.get(1).locales);

		assertEquals(5002, users.get(2).id);
		assertTrue(users.get(2).ready);
		assertEquals("FSam", users.get(2).firstName);
		assertEquals("2", users.get(2).lastName);
		assertEquals(bday, users.get(2).birthday);
		assertArrayEquals(lns, users.get(2).locales);

		assertEquals(5001, users.get(3).id);
		assertTrue(users.get(3).ready);
		assertEquals("GSam", users.get(3).firstName);
		assertEquals("1", users.get(3).lastName);
		assertEquals(bday, users.get(3).birthday);
		assertArrayEquals(lns, users.get(3).locales);
	}

	@Test
	public void testReadManyWithLimitOffset() {
		EntityDAL.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		String[] lns = new String[]{"en", "he", "ru"};
		usersToInsert.add(new User(6001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(6002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(6003L, 1, true, "ESam", "3", bday));
		usersToInsert.add(new User(6004L, 3, true, "Jim", "4", bday));
		usersToInsert.add(new User(6005L, 3, false, "Jim", "5", bday));
		usersToInsert.add(new User(6006L, 3, false, "BSam", "6", bday));
		usersToInsert.add(new User(6007L, 0, false, "ASam", "7", bday));
		usersToInsert.forEach(u -> u.locales = lns);
		int[] result = EntityDAL.of(User.class, dataSource).insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityDAL.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name", "birthday", "locales")
				.where(in("id", Arrays.asList(6001, 6003, 6004, 6007)))
				.orderBy(OrderBy.asc("state"), OrderBy.desc("last_name"))
				.read(1, 2);

		EntityDAL.of(User.class, dataSource).delete();

		assertNotNull(users);
		assertEquals(2, users.size());
		assertEquals(6001, users.get(0).id);
		assertEquals(0, (long) users.get(0).status);
		assertEquals("GSam", users.get(0).firstName);
		assertEquals("1", users.get(0).lastName);
		assertEquals(bday, users.get(0).birthday);
		assertArrayEquals(lns, users.get(0).locales);

		assertEquals(6003, users.get(1).id);
		assertEquals(1, (long) users.get(1).status);
		assertEquals("ESam", users.get(1).firstName);
		assertEquals("3", users.get(1).lastName);
		assertEquals(bday, users.get(1).birthday);
		assertArrayEquals(lns, users.get(1).locales);
	}

	@Test
	public void negativeA() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(User.class, dataSource).select()
		);
	}

	@Test
	public void negativeB() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(User.class, dataSource).select((String[]) null)
		);
	}

	@Test
	public void negativeC() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					String[] arg = new String[0];
					EntityDAL.of(User.class, dataSource).select(arg);
				}
		);
	}

	@Test
	public void negativeD() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					String[] arg = new String[]{"some", null};
					EntityDAL.of(User.class, dataSource).select(arg);
				}
		);
	}

	@Test
	public void negativeE() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					String[] arg = new String[]{"", "some"};
					EntityDAL.of(User.class, dataSource).select(arg);
				}
		);
	}

	@Test
	public void negativeF() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					String[] arg = new String[]{null, "some"};
					EntityDAL.of(User.class, dataSource).select(arg);
				}
		);
	}

	@Test
	public void negativeG() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					Set<String> fields = new HashSet<>();
					EntityDAL.of(User.class, dataSource).select(fields);
				}
		);
	}

	@Test
	public void negativeH() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					Set<String> fields = Collections.singleton(null);
					EntityDAL.of(User.class, dataSource).select(fields);
				}
		);
	}

	@Test
	public void negativeI() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					Set<String> fields = Collections.singleton("");
					EntityDAL.of(User.class, dataSource).select(fields);
				}
		);
	}

	@Test
	public void negativeJ1() {
		User r = EntityDAL.of(User.class, dataSource).select("id").where(eq("id", 9999)).readSingle();
		assertNull(r);
	}

	@Test
	public void negativeJ2() {
		assertThrows(
				IllegalStateException.class,
				() -> {
					List<User> ul = new ArrayList<>();
					ul.add(new User(7000L, 4, false, "Sam", "1", null));
					ul.add(new User(7000L, 4, false, "Sam", "1", null));
					int[] ir = EntityDAL.of(User.class, dataSource).insert(ul);
					assertTrue(IntStream.of(ir).allMatch(i -> i == 1));

					EntityDAL.of(User.class, dataSource)
							.select("id")
							.where(or(eq("ready", false), eq("ready", true), isNull("ready"), isNotNull("ready")))
							.readSingle();

				}
		);
	}

	@Test
	public void negativeK1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(0)
		);
	}

	@Test
	public void negativeK2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(0, 1)
		);
	}

	@Test
	public void negativeK3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(1, 0)
		);
	}

	@Entity
	@Table(name = TABLE_NAME)
	public static final class User {
		@Column(updatable = false, nullable = false)
		public long id;
		@Column(name = "state")
		public int status;
		@Column(name = "ready")
		public boolean ready;
		@Column(name = "first_name")
		public String firstName;
		@Column(name = "last_name")
		public String lastName;
		@Column
		public LocalDate birthday;
		@Column(name = "created", updatable = false)
		public LocalDateTime created;
		@Column
		public LocalDateTime updated;
		@Column(name = "locales")
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
