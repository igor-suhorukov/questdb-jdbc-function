package io.questdb.griffin.engine.functions.jdbc;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

public class JdbcFunctionFactory implements FunctionFactory {
    private static final IntIntHashMap jdbcToQuestColumnType = new IntIntHashMap();

    static {
        jdbcToQuestColumnType.put(Types.VARCHAR, ColumnType.STRING);
        jdbcToQuestColumnType.put(Types.NVARCHAR, ColumnType.STRING);
        jdbcToQuestColumnType.put(Types.LONGVARCHAR, ColumnType.STRING);
        jdbcToQuestColumnType.put(Types.LONGNVARCHAR, ColumnType.STRING);
        jdbcToQuestColumnType.put(Types.TIMESTAMP, ColumnType.TIMESTAMP);
        jdbcToQuestColumnType.put(Types.TIMESTAMP_WITH_TIMEZONE, ColumnType.TIMESTAMP);
        jdbcToQuestColumnType.put(Types.TIME, ColumnType.TIMESTAMP);
        jdbcToQuestColumnType.put(Types.DOUBLE, ColumnType.DOUBLE);
        jdbcToQuestColumnType.put(Types.FLOAT, ColumnType.FLOAT);
        jdbcToQuestColumnType.put(Types.REAL, ColumnType.FLOAT);
        jdbcToQuestColumnType.put(Types.INTEGER, ColumnType.INT);
        jdbcToQuestColumnType.put(Types.SMALLINT, ColumnType.SHORT);
        jdbcToQuestColumnType.put(Types.BIGINT, ColumnType.LONG);
        jdbcToQuestColumnType.put(Types.BOOLEAN, ColumnType.BOOLEAN);
        jdbcToQuestColumnType.put(Types.DATE, ColumnType.DATE);
        jdbcToQuestColumnType.put(Types.BINARY, ColumnType.BINARY);
        jdbcToQuestColumnType.put(Types.LONGVARBINARY, ColumnType.BINARY);
        jdbcToQuestColumnType.put(Types.VARBINARY, ColumnType.BINARY);
        //jdbcToQuestColumnType.put(ColumnType.BYTE);
        //jdbcToQuestColumnType.put(ColumnType.SYMBOL);
    }

    private static final NullColumn NULL = NullColumn.INSTANCE;

    @Override
    public String getSignature() {
        return "jdbc(SS)";
    }

    @Override
    @SneakyThrows
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        final CharSequence dataSourceName = args.getQuick(0).getStr(null);
        final CharSequence query = args.getQuick(1).getStr(null);
        DataSource dataSource = ConnectionFunctionFactory.getDataSource(String.valueOf(dataSourceName));
        StatementHolder statementHolder = new StatementHolder(dataSource, String.valueOf(query), true);
        final GenericRecordMetadata metadata = getResultSetMetadata(statementHolder.getResultSet());
        return new CursorFunction(
                position,
                new GenericRecordCursorFactory(metadata, new JdbcRecordCursor(statementHolder), false)
        );
    }

    private GenericRecordMetadata getResultSetMetadata(ResultSet resultSet) throws SQLException, SqlException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int columnIdx = 1; columnIdx <= metaData.getColumnCount(); columnIdx++) {
            int columnType = jdbcToQuestColumnType.get(metaData.getColumnType(columnIdx));
            if (columnType == -1) {
                throw SqlException.$(1,"JDBC column type isn't supported ").
                        put(metaData.getColumnTypeName(columnIdx));
            }
            metadata.add(new TableColumnMetadata(metaData.getColumnName(columnIdx), columnType));
        }
        return metadata;
    }

    static class JdbcRecordCursor implements NoRandomAccessRecordCursor {

        private final JdbcRecord record;

        JdbcRecordCursor(StatementHolder statementHolder) {
            record = new JdbcRecord(statementHolder);
        }

        @Override
        @SneakyThrows
        public void close() {
            record.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return record.next();
        }


        @Override
        public void toTop() {
            record.init();
        }

        @Override
        public long size() {
            return -1;
        }
    }

    static class JdbcRecord implements Record, Closeable {
        private StatementHolder statementHolder;

        JdbcRecord(StatementHolder statementHolder) {
            this.statementHolder = statementHolder;
        }

        @SneakyThrows
        void init() {
            statementHolder.createUnlimitedResultSet();
        }

        @Override
        @SneakyThrows
        public boolean getBool(int col) {
            boolean val = statementHolder.getResultSet().getBoolean(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getBool(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public byte getByte(int col) {
            byte val = statementHolder.getResultSet().getByte(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getByte(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public long getDate(int col) {
            Date date = statementHolder.getResultSet().getDate(col + 1);
            return date != null ? date.getTime() : NULL.getLong(col);
        }

        @Override
        @SneakyThrows
        public double getDouble(int col) {
            double val = statementHolder.getResultSet().getDouble(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getDouble(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public float getFloat(int col) {
            float val = statementHolder.getResultSet().getFloat(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getFloat(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public int getInt(int col) {
            int val = statementHolder.getResultSet().getInt(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getInt(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public long getLong(int col) {
            long val = statementHolder.getResultSet().getLong(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getLong(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public short getShort(int col) {
            short val = statementHolder.getResultSet().getShort(col + 1);
            if (statementHolder.getResultSet().wasNull()) {
                return NULL.getShort(col);
            }
            return val;
        }

        @Override
        @SneakyThrows
        public CharSequence getStr(int col) {
            return statementHolder.getResultSet().getString(col + 1);
        }

        @Override
        @SneakyThrows
        public long getTimestamp(int col) {
            Timestamp timestamp = statementHolder.getResultSet().getTimestamp(col + 1);
            return timestamp != null ? timestamp.getTime() : NULL.getLong(col);
        }

        @SneakyThrows
        boolean next() {
            return statementHolder.getResultSet().next();
        }

        @Override
        public void close() throws IOException {
            statementHolder.close();
        }
    }
}
