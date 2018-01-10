package org.apache.jackrabbit.vault.fs.io;

import java.io.IOException;
import java.io.OutputStream;


public interface Compression<EntryType extends Compression.Entry> {

    CompressionOutputStream<EntryType, ?> newCompressionOutputStream(OutputStream outputStream) throws IOException;
    EntryType newEntry(String name);

    interface Entry {

        String getName();

        long getTime();

        void setTime(long timestamp);

        long getCompressedSize();

        void setCompressedSize(long size);
    }

    abstract class CompressionOutputStream<EntryType, StreamType extends OutputStream> extends OutputStream {

        protected final StreamType target;

        protected CompressionOutputStream(StreamType target) {
            this.target = target;
        }

        public abstract void setLevel(int compressionLevel);

        public abstract void putNextEntry(EntryType e) throws IOException;

        public abstract void closeEntry() throws IOException;

        @Override
        public void write(byte[] b) throws IOException {
            target.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            target.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            target.flush();
        }

        @Override
        public void close() throws IOException {
            target.close();
        }

        @Override
        public void write(int b) throws IOException {
            target.write(b);
        }
    }
}
