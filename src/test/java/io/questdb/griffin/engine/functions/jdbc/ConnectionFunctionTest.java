package io.questdb.griffin.engine.functions.jdbc;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.RecordCursorPrinter;
import io.questdb.std.str.StringSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectionFunctionTest {
    @Test
    void testH2JdbcSelect(@TempDir Path tempDir) throws Exception{
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(tempDir.toAbsolutePath().toString());
        try (CairoEngine engine = new CairoEngine(configuration)){
            try (SqlCompiler compiler = new SqlCompiler(engine)){
                compiler.compile("select jdbc_pool_init(select 'mem' name,'jdbc:h2:mem:test' url, " +
                        "cast('' as STRING) user, cast('' as STRING) password from long_sequence(1)) from long_sequence(1)");
                CompiledQuery query = compiler.compile("select * from jdbc('mem','select 1,2,3,4,5 as UUID_STR')");

                StringSink sink = new StringSink();
                RecordCursorPrinter printer = new RecordCursorPrinter(sink);
                RecordMetadata metadata = query.getRecordCursorFactory().getMetadata();
                try (RecordCursor cursor = query.getRecordCursorFactory().getCursor()){
                    printer.print(cursor, metadata, true);
                    assertThat(sink.toString()).isEqualTo(
                            "1\t2\t3\t4\tUUID_STR\n" +
                                    "1\t2\t3\t4\t5\n");
                }
            }
        }
    }
}
