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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.jackrabbit.vault.fs.api.Artifact;
import org.apache.jackrabbit.vault.fs.api.VaultFile;
import org.apache.jackrabbit.vault.fs.impl.io.CompressionUtil;
import org.apache.jackrabbit.vault.util.PlatformNameFormat;

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_COMPRESSION;

/**
 * Implements a Vault filesystem exporter that exports Vault files to a jar file.
 * The entries are stored compressed in the jar (as {@link ZipArchiveEntry} zip entries.
 * <p>
 * The exporter can optimize the export throughput for binaries, by avoiding to
 * compress incompressible binaries.
 * The optimization is enabled for all {@link java.util.zip.Deflater} compression levels but
 * {@link java.util.zip.Deflater#DEFAULT_COMPRESSION}, {@link java.util.zip.Deflater#NO_COMPRESSION} and
 * {@link java.util.zip.Deflater#BEST_COMPRESSION}.
 * <p>
 * The exporter uses the {@link PlatformNameFormat} for formatting the jcr file
 * names to local ones.
 */
public class JarExporter extends AbstractExporter {

    /**
     * Contains the compression levels for which the binaries are always compressed
     * independently of their actual compressibility.
     */
    private static final Set<Integer> COMPRESSED_LEVELS = new HashSet<Integer>(Arrays.asList(
            DEFAULT_COMPRESSION, NO_COMPRESSION, BEST_COMPRESSION));

    private JarArchiveOutputStream jOut;

    private OutputStream out;

    private File jarFile;

    private final int level;

    private final boolean compressedLevel;

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     */
    public JarExporter(File jarFile) {
        this(jarFile, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     * @param level   level the compression level
     */
    public JarExporter(File jarFile, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.jarFile = jarFile;
        this.level = level;

    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out the output stream
     */
    public JarExporter(OutputStream out) {
        this(out, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out   the output stream
     * @param level level the compression level
     */
    public JarExporter(OutputStream out, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.out = out;
        this.level = level;
    }

    /**
     * Opens the exporter and initializes the undelying structures.
     *
     * @throws IOException if an I/O error occurs
     */
    public void open() throws IOException {
        if (jOut == null) {
            if (jarFile != null) {
                jOut = new JarArchiveOutputStream(new FileOutputStream(jarFile));
                jOut.setLevel(level);
            } else if (out != null) {
                jOut = new JarArchiveOutputStream(out);
                jOut.setLevel(level);
            } else {
                throw new IllegalArgumentException("Either out or jarFile needs to be set.");
            }
        }
    }

    public void close() throws IOException {
        if (jOut != null) {
            jOut.close();
            jOut = null;
        }
    }

    public void createDirectory(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        ZipArchiveEntry e = new ZipArchiveEntry(getPlatformFilePath(file, relPath) + "/");
        jOut.putArchiveEntry(e);
        jOut.closeArchiveEntry();
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void createDirectory(String relPath) throws IOException {
        ZipArchiveEntry e = new ZipArchiveEntry(relPath + "/");
        jOut.putArchiveEntry(e);
        jOut.closeArchiveEntry();
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void writeFile(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        ZipArchiveEntry e = new ZipArchiveEntry(getPlatformFilePath(file, relPath));
        Artifact a = file.getArtifact();
        boolean compress = compressedLevel || CompressionUtil.isCompressible(a) >= 0;
        if (!compress) {
            jOut.setLevel(NO_COMPRESSION);
        }
        if (a.getLastModified() > 0) {
            e.setTime(a.getLastModified());
        }
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        jOut.putArchiveEntry(e);
        switch (a.getPreferredAccess()) {
            case NONE:
                throw new RepositoryException("Artifact has no content.");

            case SPOOL:
                OutputStream nout = new CloseShieldOutputStream(jOut);
                a.spool(nout);
                break;

            case STREAM:
                nout = new CloseShieldOutputStream(jOut);
                InputStream in = a.getInputStream();
                IOUtils.copy(in, nout);
                in.close();
                break;
        }
        jOut.closeArchiveEntry();
        if (!compress) {
            jOut.setLevel(level);
        }
    }

    public void writeFile(InputStream in, String relPath) throws IOException {
        // The file input stream to be written is assumed to be compressible
        ZipArchiveEntry e = new ZipArchiveEntry(relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        jOut.putArchiveEntry(e);
        OutputStream nout = new CloseShieldOutputStream(jOut);
        IOUtils.copy(in, nout);
        in.close();
        jOut.closeArchiveEntry();
    }

    public void write(Archive archive, Archive.Entry entry) throws IOException {
        if (archive instanceof ZipArchive) {
            ZipFile zipFile = ((ZipArchive) archive).getZipFile();
            ZipArchiveEntry zipEntry = zipFile.getEntry(entry.getRelPath());
            if (zipEntry == null) {
                // the entry doesn't exists, so it might have been created as intermediate of an existing entry.
                // as we are copying entries from one archive to another, we skip that case as well
                return;
            }
            track("A", entry.getName());
            if (!compressedLevel) {
                // The entry to be written is assumed to be incompressible
                jOut.setLevel(NO_COMPRESSION);
            }
            exportInfo.update(ExportInfo.Type.ADD, entry.getName());
            ZipArchiveEntry copy = new ZipArchiveEntry(zipEntry);
            copy.setCompressedSize(-1);
            jOut.putArchiveEntry(copy);
            if (!entry.isDirectory()) {
                // copy
                InputStream in = zipFile.getInputStream(zipEntry);
                IOUtils.copy(in, jOut);
                in.close();
            }
            jOut.closeArchiveEntry();
            if (!compressedLevel) {
                jOut.setLevel(level);
            }
        } else {
            super.write(archive, entry);
        }
    }


}