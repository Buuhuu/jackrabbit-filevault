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

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_COMPRESSION;

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
public class JarExporter extends CompressionExporter<JarEntry, JarCompression> {

    /**
     * Contains the compression levels for which the binaries are always compressed
     * independently of their actual compressibility.
     */
    private static final Set<Integer> COMPRESSED_LEVELS = new HashSet<Integer>(Arrays.asList(
            DEFAULT_COMPRESSION, NO_COMPRESSION, BEST_COMPRESSION));

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
        super(jarFile, new JarCompression(), level);
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
        super(out, new JarCompression(), level);
    }

    public void write(Archive archive, Archive.Entry entry) throws IOException {
        if (archive instanceof ZipArchive) {
            ZipFile zipFile = ((ZipArchive) archive).getZipFile();
            ZipEntry zipEntry = zipFile.getEntry(entry.getRelPath());
            if (zipEntry == null) {
                // the entry doesn't exists, so it might have been created as intermediate of an existing entry.
                // as we are copying entries from one archive to another, we skip that case as well
                return;
            }
            track("A", entry.getName());
            if (!compressedLevel) {
                // The entry to be written is assumed to be incompressible
                cOut.setLevel(NO_COMPRESSION);
            }
            exportInfo.update(ExportInfo.Type.ADD, entry.getName());
            JarEntry copy = compression.newEntry(zipEntry);
            copy.setCompressedSize(-1);
            cOut.putNextEntry(copy);
            if (!entry.isDirectory()) {
                // copy
                InputStream in = zipFile.getInputStream(zipEntry);
                IOUtils.copy(in, cOut);
                in.close();
            }
            cOut.closeEntry();
            if (!compressedLevel) {
                cOut.setLevel(level);
            }
        } else {
            super.write(archive, entry);
        }
    }
}