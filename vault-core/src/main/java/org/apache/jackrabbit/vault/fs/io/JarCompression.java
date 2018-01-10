package org.apache.jackrabbit.vault.fs.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

class JarEntry implements Compression.Entry {

    private final ZipEntry zipEntry;

    JarEntry(String name) {
        this.zipEntry = new ZipEntry(name);
    }

    JarEntry(ZipEntry entry) {
        this.zipEntry = new ZipEntry(entry);
    }

    ZipEntry getZipEntry() {
        return zipEntry;
    }

    @Override public String getName() {
        return zipEntry.getName();
    }

    @Override public long getTime() {
        return zipEntry.getTime();
    }

    @Override
    public void setTime(long timestamp) {
        zipEntry.setTime(timestamp);
    }
    @Override
    public long getCompressedSize() {
        return zipEntry.getCompressedSize();
    }

    @Override
    public void setCompressedSize(long size) {
        zipEntry.setCompressedSize(size);
    }
}

class JarCompressionOutputStream extends Compression.CompressionOutputStream<JarEntry, JarOutputStream> {

    JarCompressionOutputStream(OutputStream target) throws IOException {
        super(new JarOutputStream(target));
    }

    @Override
    public void setLevel(int compressionLevel) {
        target.setLevel(compressionLevel);
    }

    @Override
    public void putNextEntry(JarEntry e) throws IOException {
        target.putNextEntry(e.getZipEntry());
    }

    @Override
    public void closeEntry() throws IOException {
        target.closeEntry();
    }
}

public class JarCompression implements Compression<JarEntry> {

    @Override
    public CompressionOutputStream newCompressionOutputStream(OutputStream outputStream) throws IOException {
        return new JarCompressionOutputStream(outputStream);
    }

    @Override
    public JarEntry newEntry(String name) {
        return new JarEntry(name);
    }

    public JarEntry newEntry(ZipEntry entry) {
        return new JarEntry(entry);
    }
}
