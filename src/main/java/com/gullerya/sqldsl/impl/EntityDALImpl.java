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
import com.gullerya.sqldsl.api.statements.Delete;
import com.gullerya.sqldsl.api.statements.Insert;
import com.gullerya.sqldsl.api.statements.Select;
import com.gullerya.sqldsl.api.statements.Update;

public class EntityDALImpl<T> implements EntityDAL<T> {
	private final Delete<T> stmntDelete;
	private final Insert<T> stmntInsert;
	private final Select<T> stmntSelect;
	private final Update<T> stmntUpdate;

	public EntityDALImpl(Class<T> entityType, DataSource ds) throws ReflectiveOperationException {
		EntityMetaProc<T> em = new EntityMetaProc<>(entityType);
		ESConfig<T> config = new ESConfig<>(ds, em);
		stmntDelete = new StatementDeleteImpl<>(config);
		stmntInsert = new StatementInsertImpl<>(config);
		stmntSelect = new StatementSelectImpl<>(config);
		stmntUpdate = new StatementUpdateImpl<>(config);
	}

	@Override
	public int delete() {
		return stmntDelete.delete();
	}

	@Override
	public int delete(Where.WhereClause whereClause) {
		return stmntDelete.delete(whereClause);
	}

	@Override
	public int insert(T entity, Literal... literals) {
		return stmntInsert.insert(entity, literals);
	}

	@Override
	public int[] insert(Collection<T> entities, Literal... literals) {
		return stmntInsert.insert(entities, literals);
	}

	@Override
	public SelectDownstream<T> select(String... fields) {
		return stmntSelect.select(fields);
	}

	@Override
	public SelectDownstream<T> select(Set<String> fields) {
		return stmntSelect.select(fields);
	}

	@Override
	public UpdateDownstream update(T entity, Literal... literals) {
		return stmntUpdate.update(entity, literals);
	}

	public record ESConfig<ET>(DataSource ds, EntityMetaProc<ET> em) {
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
