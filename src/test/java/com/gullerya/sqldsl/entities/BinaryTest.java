package com.gullerya.sqldsl.entities;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.Where;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BinaryTest {
	private static final String TABLE_NAME = "BinaryTestTable";
	private static final DataSource dataSource = DBUtils.getDataSource();
	private static final EntityDAL<BinaryContainer> binConService = EntityDAL.of(BinaryContainer.class, dataSource);

	@BeforeAll
	public static void prepare() throws Exception {
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + TABLE_NAME + "\" (" +
								"   id      BIGINT," +
								"   barray  BYTEA," +
								"   stream  BYTEA" +
								")"
				)
				.execute();
	}

	@Test
	public void createSingle() {
		binConService.delete();

		//  insert single
		byte[] av = new byte[]{1, 2, 3, 4, 5, 6, 7};
		byte[] sv = new byte[]{6, 5, 4, 3, 2, 1};
		BinaryContainer bc = new BinaryContainer();
		bc.id = 1000L;
		bc.barray = av;
		bc.stream = new ByteArrayInputStream(sv);
		assertEquals(1, binConService.insert(bc));

		//  read single and verify
		BinaryContainer br = binConService
				.select("barray", "stream")
				.where(Where.eq("id", 1000))
				.readSingle();
		assertNotNull(br);
		assertArrayEquals(av, br.barray);
		assertArrayEquals(sv, isToBytes(br.stream));
	}

	@Test
	public void updateSingle() {
		binConService.delete();

		//  insert single
		byte[] av1 = new byte[]{1, 2, 3, 4, 5, 6, 7};
		byte[] sv1 = new byte[]{6, 5, 4, 3, 2, 1};
		BinaryContainer bc = new BinaryContainer();
		bc.id = 2000L;
		bc.barray = av1;
		bc.stream = new ByteArrayInputStream(sv1);
		assertEquals(1, binConService.insert(bc));

		//  update single
		byte[] av2 = new byte[]{2, 3, 4, 5, 6};
		byte[] sv2 = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1};
		BinaryContainer bu = new BinaryContainer();
		bu.barray = av2;
		bu.stream = new ByteArrayInputStream(sv2);
		assertEquals(1, (int) binConService.update(bu).where(Where.eq("id", 2000)));

		//  read single and verify
		BinaryContainer br = binConService.select("barray", "stream").where(Where.eq("id", 2000)).readSingle();
		assertNotNull(br);
		assertArrayEquals(av2, br.barray);
		assertArrayEquals(sv2, isToBytes(br.stream));
	}

	@Test
	public void createSeveral() {
		binConService.delete();

		//  insert several
		List<BinaryContainer> csl = new ArrayList<>();
		BinaryContainer bc = new BinaryContainer();
		bc.id = 3000L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 3001L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 3002L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		assertArrayEquals(new int[]{1, 1, 1}, binConService.insert(csl));

		//  read several and verify
		List<BinaryContainer> brs = binConService.select("id", "barray", "stream").read();
		assertNotNull(brs);
		assertEquals(3, brs.size());
		for (BinaryContainer br : brs) {
			assertNotNull(br.id);
			assertArrayEquals(ByteUtils.longToBytes(br.id), br.barray);
			assertArrayEquals(ByteUtils.longToBytes(br.id), isToBytes(br.stream));
		}
	}

	@Test
	public void updateSeveral() {
		binConService.delete();

		//  insert several
		List<BinaryContainer> csl = new ArrayList<>();
		BinaryContainer bc = new BinaryContainer();
		bc.id = 4000L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 4001L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 4002L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		assertArrayEquals(new int[]{1, 1, 1}, binConService.insert(csl));

		//  update several
		BinaryContainer bu = new BinaryContainer();
		bu.barray = new byte[]{0, 0};
		assertEquals(3, binConService.update(bu).all());

		//  read several and verify
		List<BinaryContainer> brs = binConService.select("id", "barray", "stream").read();
		assertNotNull(brs);
		assertEquals(3, brs.size());
		for (BinaryContainer br : brs) {
			assertNotNull(br.id);
			assertArrayEquals(new byte[]{0, 0}, br.barray);
			assertArrayEquals(ByteUtils.longToBytes(br.id), isToBytes(br.stream));
		}
	}

	@Entity
	@Table(name = TABLE_NAME)
	public static final class BinaryContainer {
		@Column(updatable = false)
		public Long id;
		@Column
		public byte[] barray;
		@Column
		public InputStream stream;
	}

	private byte[] isToBytes(InputStream is) {
		if (is == null) {
			throw new IllegalArgumentException("input stream MUST NOT be NULL");
		}

		byte[] buffer = new byte[4096];
		int length;
		try (ByteArrayOutputStream result = new ByteArrayOutputStream()) {
			while ((length = is.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}
			return result.toByteArray();
		} catch (IOException ioe) {
			throw new RuntimeException("failed to read input stream to bytes", ioe);
		}
	}

	private static final class ByteUtils {
		private static final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

		public static byte[] longToBytes(long x) {
			buffer.putLong(0, x);
			return buffer.array().clone();
		}

		public static long bytesToLong(byte[] bytes) {
			buffer.put(bytes, 0, bytes.length);
			buffer.flip();
			return buffer.getLong();
		}
	}
}
