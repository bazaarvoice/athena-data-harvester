package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import com.fasterxml.jackson.databind.SequenceWriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

public class WrappedSequenceWriter implements Closeable, Flushable {

    private final SequenceWriter _sequenceWriter;
    private final Consumer<Boolean> _onCloseDataWritten;

    private boolean _dataWritten = false;

    public WrappedSequenceWriter(SequenceWriter sequenceWriter, Consumer<Boolean> onCloseDataWritten) {
        _sequenceWriter = sequenceWriter;
        _onCloseDataWritten = onCloseDataWritten;
    }

    public WrappedSequenceWriter writeAll(Collection<?> collection)
            throws IOException {
        _dataWritten = _dataWritten || !collection.isEmpty();
        _sequenceWriter.writeAll(collection);
        return this;
    }

    @Override
    public void close()
            throws IOException {
        _sequenceWriter.close();
        _onCloseDataWritten.accept(_dataWritten);
    }

    @Override
    public void flush()
            throws IOException {
        _sequenceWriter.flush();
    }
}
