package io.questdb.griffin.engine.functions.jdbc;

import lombok.Getter;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

public class StatementHolder implements Closeable {
    private final DataSource dataSource;
    private final String query;
    private Connection connection;
    private PreparedStatement statement;
    @Getter
    private ResultSet resultSet;

    StatementHolder(DataSource dataSource, String query, boolean schemaOnly) throws SQLException {
        this.dataSource = dataSource;
        this.query = query;
        init(schemaOnly);
    }

    private void init(boolean schemaOnly) throws SQLException {
        this.connection = this.dataSource.getConnection();
        this.statement = connection.prepareStatement(this.query);
        if(schemaOnly){
            statement.setMaxRows(0);
        }
        try {
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            try (Connection ignored = this.connection){
                statement.close();
            }
            throw e;
        }
    }

    @SneakyThrows
    public void createUnlimitedResultSet(){
        if(connection == null){
            init(false);
        }
        if(statement.getLargeMaxRows() == 0L){
            try {
                statement.setLargeMaxRows(Long.MAX_VALUE);
                resultSet.close();
                resultSet=statement.executeQuery();
            } catch (SQLException e) {
                close();
                throw e;
            }
        }
    }

    @Override
    @SneakyThrows
    public void close() throws IOException {
        try (Connection ignoredConnection = connection){
            try (Statement ignored = statement){
                resultSet.close();
            }
        } finally {
            connection = null;
            statement = null;
            resultSet=null;
        }
    }
}

