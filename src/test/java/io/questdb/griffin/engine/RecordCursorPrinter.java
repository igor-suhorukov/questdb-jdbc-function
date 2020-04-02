package io.questdb.griffin.engine;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.str.CharSink;
import io.questdb.std.time.DateFormatUtils;

public class RecordCursorPrinter {
    private final CharSink sink;
    private final char delimiter;

    public RecordCursorPrinter(CharSink sink) {
        this.sink = sink;
        this.delimiter = '\t';
    }

    public void print(RecordCursor cursor, RecordMetadata metadata, boolean header) {
        if (header) {
            printHeader(metadata);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            print(record, metadata);
        }
    }

    public void print(Record r, RecordMetadata m) {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            printColumn(r, m, i);
        }
        sink.put("\n");
        sink.flush();
    }

    public void printHeader(RecordMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            sink.put(metadata.getColumnName(i));
        }
        sink.put('\n');
    }

    private void printColumn(Record r, RecordMetadata m, int i) {
        switch (m.getColumnType(i)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, r.getDate(i));
                break;
            case ColumnType.TIMESTAMP:
                io.questdb.std.microtime.DateFormatUtils.appendDateTimeUSec(sink, r.getTimestamp(i));
                break;
            case ColumnType.DOUBLE:
                sink.put(r.getDouble(i));
                break;
            case ColumnType.FLOAT:
                sink.put(r.getFloat(i), 4);
                break;
            case ColumnType.INT:
                sink.put(r.getInt(i));
                break;
            case ColumnType.STRING:
                r.getStr(i, sink);
                break;
            case ColumnType.SYMBOL:
                sink.put(r.getSym(i));
                break;
            case ColumnType.SHORT:
                sink.put(r.getShort(i));
                break;
            case ColumnType.CHAR:
                char c = r.getChar(i);
                if (c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                sink.put(r.getLong(i));
                break;
            case ColumnType.BYTE:
                sink.put(r.getByte(i));
                break;
            case ColumnType.BOOLEAN:
                sink.put(r.getBool(i));
                break;
            case ColumnType.BINARY:
                Chars.toSink(r.getBin(i), sink);
                break;
            case ColumnType.LONG256:
                r.getLong256(i, sink);
                break;
            default:
                break;
        }
    }
}
