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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;

public class TarStreamArchive extends AbstractStreamArchive {

    private static final Logger LOG = LoggerFactory.getLogger(AutodetectStreamArchive.class);
    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = new CompressorStreamFactory();

    private final InputStream in;
    private final String compression;

    /**
     * Creates an new archive stream archive on the given input stream.
     *
     * @param in the input stream to read from.
     */
    public TarStreamArchive(@Nonnull InputStream in, @Nonnull String compression) {
        requireNonDeflate(compression);
        this.in = in;
        this.compression = compression;
    }

    /**
     * Creates an ew archive stream archive on the given input stream.
     *
     * @param in            the input stream to read from.
     * @param maxBufferSize size of buffer to keep content in memory.
     */
    public TarStreamArchive(@Nonnull InputStream in, @Nonnull String compression, int maxBufferSize) {
        super(maxBufferSize);
        requireNonDeflate(compression);
        this.in = in;
        this.compression = compression;
    }

    @Override
    protected ArchiveInputStream openArchiveInputStream() throws IOException {
        return getDecompressedStream(compression, in);
    }

    protected static void requireNonDeflate(String compression) {
        if ("deflate".equals(compression)) {
            throw new IllegalArgumentException("deflate is not supported for tar, use zip in that case.");
        }
    }

    protected static TarArchiveInputStream getDecompressedStream(String compression, InputStream in) throws IOException {
        try {

            if (!(in instanceof BufferedInputStream)) {
                in = new BufferedInputStream(in);
            }

            if ("snappy-framed".equals(compression)) {
                try {
                    in = new SnappyFramedInputStream(in);
                } catch (NoClassDefFoundError ex) {
                    LOG.trace("Native (JNI) snappy implementation not found.", ex);
                }
            } else {
                in = COMPRESSOR_STREAM_FACTORY.createCompressorInputStream(compression, in);
            }

            return new TarArchiveInputStream(in);
        } catch (CompressorException ex) {
            throw new IOException("Unknown format.");
        }
    }
}
