package org.apache.jackrabbit.vault.packaging;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.jackrabbit.vault.fs.io.AbstractExporter;

public interface ExporterFactory {

    AbstractExporter createExporter(OutputStream outputStream, String compressionMethod, int compressionLevel) throws IOException;
}
