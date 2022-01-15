package com.gullerya.sqldsl.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import javax.sql.DataSource;

import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.clauses.Where;

public class EntityDALImpl<T> implements EntityDAL<T> {
	private final ESConfig<T> config;

	public EntityDALImpl(Class<T> entityType, DataSource ds) throws ReflectiveOperationException {
		EntityMetaProc<T> em = new EntityMetaProc<>(entityType);
		this.config = new ESConfig<>(ds, em);
	}

	@Override
	public int deleteAll() {
		return new StatementDeleteImpl<>(config).deleteAll();
	}

	@Override
	public int deleteAll(Where.WhereClause whereClause) {
		return new StatementDeleteImpl<>(config).deleteAll(whereClause);
	}

	@Override
	public int insert(T entity, Literal... literals) {
		return new StatementInsertImpl<>(config).insert(entity, literals);
	}

	@Override
	public int[] insert(Collection<T> entities, Literal... literals) {
		return new StatementInsertImpl<>(config).insert(entities, literals);
	}

	@Override
	public SelectDownstream<T> select(String... fields) {
		return new StatementSelectImpl<>(config).select(fields);
	}

	@Override
	public SelectDownstream<T> select(Set<String> fields) {
		return new StatementSelectImpl<>(config).select(fields);
	}

	@Override
	public UpdateDownstream update(T entity, Literal... literals) {
		return new StatementUpdateImpl<>(config).update(entity, literals);
	}

	record ESConfig<ET>(DataSource ds, EntityMetaProc<ET> em) {
		<R> R prepareStatementAndDo(String sql, PreparedStatementAction<R> psAction) {
			try (Connection c = ds.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
				return psAction.execute(s);
			} catch (SQLException sqle) {
				throw new IllegalStateException(sqle);
			}
		}
	}

	interface PreparedStatementAction<R> {
		R execute(PreparedStatement s) throws SQLException;
	}
}
