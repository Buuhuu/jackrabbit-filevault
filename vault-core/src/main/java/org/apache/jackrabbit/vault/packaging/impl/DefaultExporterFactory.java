package org.apache.jackrabbit.vault.packaging.impl;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jackrabbit.vault.fs.io.AbstractExporter;
import org.apache.jackrabbit.vault.fs.io.JarExporter;
import org.apache.jackrabbit.vault.fs.io.TarExporter;
import org.apache.jackrabbit.vault.packaging.ExporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedOutputStream;

public class DefaultExporterFactory implements ExporterFactory {

    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = new CompressorStreamFactory();
    private static final Logger LOG = LoggerFactory.getLogger(DefaultExporterFactory.class);

    @Override
    public AbstractExporter createExporter(@Nonnull OutputStream outputStream, @Nonnull String compressionMethod, int compressionLevel)
            throws IOException {
        try {
            if ("deflate".equals(compressionMethod)) {
                return new JarExporter(outputStream, compressionLevel);
            }

            if ("snappy-framed".equals(compressionMethod)) {
                try {
                    new TarExporter(new SnappyFramedOutputStream(outputStream));
                } catch (NoClassDefFoundError ex) {
                    LOG.trace("Native (JNI) snappy implementation not found.", ex);
                }
            }

            return new TarExporter(COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressionMethod, outputStream));
        } catch (CompressorException ex) {
            throw new IllegalArgumentException("Unknown compression format.");
        }
    }
}
