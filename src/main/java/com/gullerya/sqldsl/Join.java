package com.gullerya.sqldsl;

import com.gullerya.sqldsl.api.statements.Select;

public interface Join {

	<T, K> void inner(Select.SelectDownstream<T> selectionA, Select.SelectDownstream<K> selectionB);

	<T, K> void toward(Class<T> mainEntity, Select.SelectDownstream<T> selectionA, Select.SelectDownstream<K> selectionB);

}
