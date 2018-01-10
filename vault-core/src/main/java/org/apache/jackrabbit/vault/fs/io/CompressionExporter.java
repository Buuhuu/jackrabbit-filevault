/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.vault.fs.io;

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_COMPRESSION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.jcr.RepositoryException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.jackrabbit.vault.fs.api.Artifact;
import org.apache.jackrabbit.vault.fs.api.VaultFile;
import org.apache.jackrabbit.vault.fs.impl.io.CompressionUtil;
import org.apache.jackrabbit.vault.util.PlatformNameFormat;

/**
 * Implements a Vault filesystem exporter that exports Vault files to a jar file.
 * The entries are stored compressed in the jar (as {@link ZipEntry} zip entries.
 * <p>
 * The exporter can optimize the export throughput for binaries, by avoiding to
 * compress incompressible binaries.
 * The optimization is enabled for all {@link Deflater} compression levels but
 * {@link Deflater#DEFAULT_COMPRESSION}, {@link Deflater#NO_COMPRESSION} and
 * {@link Deflater#BEST_COMPRESSION}.
 * <p>
 * The exporter uses the {@link PlatformNameFormat} for formatting the jcr file
 * names to local ones.
 */
public class CompressionExporter<EntryType extends Compression.Entry, CompressionType extends Compression<EntryType>> extends AbstractExporter {

    /**
     * Contains the compression levels for which the binaries are always compressed
     * independently of their actual compressibility.
     */
    private static final Set<Integer> COMPRESSED_LEVELS = new HashSet<Integer>(Arrays.asList(
            DEFAULT_COMPRESSION, NO_COMPRESSION, BEST_COMPRESSION));

    protected final CompressionType compression;

    protected Compression.CompressionOutputStream<EntryType, ?> cOut;

    protected final int level;

    protected final boolean compressedLevel;

    private OutputStream out;

    private File jarFile;

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     */
    public CompressionExporter(File jarFile, CompressionType compression) {
        this(jarFile, compression, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     * @param level   level the compression level
     */
    public CompressionExporter(File jarFile, CompressionType compression, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.jarFile = jarFile;
        this.level = level;
        this.compression = compression;
    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out the output stream
     */
    public CompressionExporter(OutputStream out, CompressionType compression) {
        this(out, compression, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out   the output stream
     * @param level level the compression level
     */
    public CompressionExporter(OutputStream out, CompressionType compression, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.out = out;
        this.level = level;
        this.compression = compression;
    }

    /**
     * Opens the exporter and initializes the undelying structures.
     *
     * @throws IOException if an I/O error occurs
     */
    public void open() throws IOException {
        if (cOut == null) {
            if (jarFile != null) {
                cOut = compression.newCompressionOutputStream(new FileOutputStream(jarFile));
                cOut.setLevel(level);
            } else if (out != null) {
                cOut = compression.newCompressionOutputStream(out);
                cOut.setLevel(level);
            } else {
                throw new IllegalArgumentException("Either out or jarFile needs to be set.");
            }
        }
    }

    public void close() throws IOException {
        if (cOut != null) {
            cOut.close();
            cOut = null;
        }
    }

    public void createDirectory(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        EntryType e = compression.newEntry(getPlatformFilePath(file, relPath) + "/");
        cOut.putNextEntry(e);
        cOut.closeEntry();
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void createDirectory(String relPath) throws IOException {
        EntryType e = compression.newEntry(relPath + "/");
        cOut.putNextEntry(e);
        cOut.closeEntry();
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void writeFile(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        EntryType e = compression.newEntry(getPlatformFilePath(file, relPath));
        Artifact a = file.getArtifact();
        boolean compress = compressedLevel || CompressionUtil.isCompressible(a) >= 0;
        if (!compress) {
            cOut.setLevel(NO_COMPRESSION);
        }
        if (a.getLastModified() > 0) {
            e.setTime(a.getLastModified());
        }
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        cOut.putNextEntry(e);
        switch (a.getPreferredAccess()) {
            case NONE:
                throw new RepositoryException("Artifact has no content.");

            case SPOOL:
                OutputStream nout = new CloseShieldOutputStream(cOut);
                a.spool(nout);
                break;

            case STREAM:
                nout = new CloseShieldOutputStream(cOut);
                InputStream in = a.getInputStream();
                IOUtils.copy(in, nout);
                in.close();
                break;
        }
        cOut.closeEntry();
        if (!compress) {
            cOut.setLevel(level);
        }
    }

    public void writeFile(InputStream in, String relPath) throws IOException {
        // The file input stream to be written is assumed to be compressible
        EntryType e = compression.newEntry(relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        cOut.putNextEntry(e);
        OutputStream nout = new CloseShieldOutputStream(cOut);
        IOUtils.copy(in, nout);
        in.close();
        cOut.closeEntry();
    }
}