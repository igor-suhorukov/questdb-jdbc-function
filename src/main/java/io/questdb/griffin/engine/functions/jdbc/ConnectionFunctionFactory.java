package io.questdb.griffin.engine.functions.jdbc;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.NullStrConstant;
import io.questdb.plugin.GlobalComponent;
import io.questdb.std.ObjList;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionFunctionFactory implements FunctionFactory, GlobalComponent {
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_DRIVER = "driver";
    private static final String COLUMN_URL = "url";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_PASSWORD = "password";
    private static final String COLUMN_SCHEMA = "schema";
    private static final String COLUMN_CATALOG = "catalog";
    private static final String COLUMN_AUTO_COMMIT = "auto_commit";
    private static final String COLUMN_READ_ONLY = "read_only";
    private static final String COLUMN_JMX = "jmx";
    private static final String COLUMN_MAX_POOL_SIZE = "max_pool_size";
    private static final Map<String, HikariDataSource> DBCP = new ConcurrentHashMap<>();
    private static final String TRANSACTION_ISOLATION = "transaction_isolation";
    private static final String IDLE_TIMEOUT = "idle_timeout";
    private static final String INITIALIZATION_FAIL_TIMEOUT = "initialization_fail_timeout";
    private static final String CONNECTION_TIMEOUT = "connection_timeout";
    private static final String MAX_LIFETIME = "max_lifetime";
    private static final String MINIMUM_IDLE = "minimum_idle";
    private static final String VALIDATION_TIMEOUT = "validation_timeout";

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> DBCP.values().forEach(HikariDataSource::close)));
    }

    private SqlCompiler sqlCompiler;

    @Override
    public String getSignature() {
        return "jdbc_pool_init(C)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration sqlConfiguration) throws SqlException {
        Function settings = args.getQuick(0);
        RecordMetadata metadata = settings.getMetadata();

        int nameIdx = getColumnIndex(metadata, COLUMN_NAME, ColumnType.STRING, true);
        int urlIdx = getColumnIndex(metadata, COLUMN_URL, ColumnType.STRING, true);
        int userIdx = getColumnIndex(metadata, COLUMN_USER, ColumnType.STRING, true);
        int passwordIdx = getColumnIndex(metadata, COLUMN_PASSWORD, ColumnType.STRING, true);
        int driverIdx = getColumnIndex(metadata, COLUMN_DRIVER, ColumnType.STRING, false);
        int schemaIdx = getColumnIndex(metadata, COLUMN_SCHEMA, ColumnType.STRING, false);
        int catalogIdx = getColumnIndex(metadata, COLUMN_CATALOG, ColumnType.STRING, false);
        int autoCommitIdx = getColumnIndex(metadata, COLUMN_AUTO_COMMIT, ColumnType.BOOLEAN, false);
        int readOnlyIdx = getColumnIndex(metadata, COLUMN_READ_ONLY, ColumnType.BOOLEAN, false);
        int jmxIdx = getColumnIndex(metadata, COLUMN_JMX, ColumnType.BOOLEAN, false);
        int maxPoolSizeIdx = getColumnIndex(metadata, COLUMN_MAX_POOL_SIZE, ColumnType.INT, false);
        int transactionIsolationIdx = getColumnIndex(metadata, TRANSACTION_ISOLATION, ColumnType.STRING, false);
        int idleTimeoutIdx = getColumnIndex(metadata, IDLE_TIMEOUT, ColumnType.LONG, false);
        int initializationFailTimeoutIdx = getColumnIndex(metadata, INITIALIZATION_FAIL_TIMEOUT, ColumnType.LONG, false);
        int connectionTimeoutIdx = getColumnIndex(metadata, CONNECTION_TIMEOUT, ColumnType.LONG, false);
        int maxLifetimeIdx = getColumnIndex(metadata, MAX_LIFETIME, ColumnType.LONG, false);
        int minimumIdleIdx = getColumnIndex(metadata, MINIMUM_IDLE, ColumnType.INT, false);
        int validationTimeoutIdx = getColumnIndex(metadata, VALIDATION_TIMEOUT, ColumnType.LONG, false);

        RecordCursor recordCursor = settings.getRecordCursorFactory().getCursor(null);
        while (recordCursor.hasNext()){
            Record record = recordCursor.getRecord();
            HikariConfig configuration = new HikariConfig();
            String poolName = String.valueOf(record.getStr(nameIdx));
            configuration.setPoolName(poolName);

            configuration.setJdbcUrl(String.valueOf(record.getStr(urlIdx)));
            configuration.setUsername(String.valueOf(record.getStr(userIdx)));
            configuration.setPassword(String.valueOf(record.getStr(passwordIdx)));

            if(driverIdx!=-1){
                configuration.setDriverClassName(String.valueOf(record.getStr(driverIdx)));
            }
            if(schemaIdx!=-1){
                configuration.setSchema(String.valueOf(record.getStr(schemaIdx)));
            }
            if(catalogIdx!=-1){
                configuration.setCatalog(String.valueOf(record.getStr(catalogIdx)));
            }
            if(autoCommitIdx!=-1){
                configuration.setAutoCommit(record.getBool(autoCommitIdx));
            }
            if(readOnlyIdx!=-1){
                configuration.setReadOnly(record.getBool(readOnlyIdx));
            }
            if(jmxIdx!=-1){
                configuration.setRegisterMbeans(record.getBool(jmxIdx));
            }
            if(maxPoolSizeIdx!=-1){
                configuration.setMaximumPoolSize(record.getInt(maxPoolSizeIdx));
            }
            if(transactionIsolationIdx!=-1){
                configuration.setTransactionIsolation(String.valueOf(record.getStr(transactionIsolationIdx)));
            }
            if(idleTimeoutIdx!=-1){
                configuration.setIdleTimeout(record.getLong(idleTimeoutIdx));
            }
            if(initializationFailTimeoutIdx!=-1){
                configuration.setInitializationFailTimeout(record.getLong(initializationFailTimeoutIdx));
            }
            if(connectionTimeoutIdx!=-1){
                configuration.setConnectionTimeout(record.getLong(connectionTimeoutIdx));
            }
            if(maxLifetimeIdx!=-1){
                configuration.setMaxLifetime(record.getLong(maxLifetimeIdx));
            }
            if(minimumIdleIdx!=-1){
                configuration.setMinimumIdle(record.getInt(minimumIdleIdx));
            }
            if(validationTimeoutIdx!=-1){
                configuration.setValidationTimeout(record.getLong(validationTimeoutIdx));
            }
            DBCP.computeIfAbsent(poolName, s -> new HikariDataSource(configuration));
        }

        return new NullStrConstant(position);
    }

    private int getColumnIndex(RecordMetadata metadata, String columnName, int expectedColumnType, boolean requered) throws SqlException {
        int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if(requered && columnIndex==-1){
            throw SqlException.invalidColumn(columnIndex,columnName).put(" not found");
        }
        if(columnIndex!=-1 && metadata.getColumnType(columnIndex) != expectedColumnType){
            throw SqlException.invalidColumn(columnIndex, columnName).put(", expected type ").put(expectedColumnType).put(" but found ").put(metadata.getColumnType(columnIndex));
        }
        return columnIndex;
    }

    static DataSource getDataSource(String dataSourceName) throws SqlException{
        HikariDataSource dataSource = DBCP.get(dataSourceName);
        if (dataSource == null) {
            throw SqlException.$(1,"DataSource ").put(dataSourceName).put(" not found");
        }
        return dataSource;
    }

    @Override
    public void init(CairoEngine cairoEngine) {
        sqlCompiler = new SqlCompiler(cairoEngine);
    }

    @Override
    public void close() throws IOException {
        sqlCompiler.close();
    }
}
