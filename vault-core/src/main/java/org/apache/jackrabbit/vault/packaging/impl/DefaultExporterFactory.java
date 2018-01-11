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

package org.apache.jackrabbit.vault.packaging.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;

import javax.annotation.Nonnull;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jackrabbit.vault.fs.io.AbstractExporter;
import org.apache.jackrabbit.vault.fs.io.JarExporter;
import org.apache.jackrabbit.vault.fs.io.TarExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedOutputStream;

public class DefaultExporterFactory {

    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = new CompressorStreamFactory();
    private static final Logger LOG = LoggerFactory.getLogger(DefaultExporterFactory.class);

    @Nonnull
    public AbstractExporter createExporter(@Nonnull OutputStream outputStream, @Nonnull String compressionMethod, int compressionLevel)
            throws IOException {
        try {
            if ("deflate".equals(compressionMethod)) {
                return new JarExporter(outputStream, compressionLevel);
            }

            if ("snappy-framed".equals(compressionMethod)) {
                try {
                    // compression ratio to achieve is between 0 and 1
                    double ratio;
                    if (compressionLevel < 0) {
                        ratio = SnappyFramedOutputStream.DEFAULT_MIN_COMPRESSION_RATIO;
                    } else {
                        // Deflater#BEST_SPEED is 1, best speed for snappy is a min ratio 1.0 - 0, so reduce the compression level by 1.
                        compressionLevel = Math.max(0, compressionLevel - 1);
                        ratio = 1.0 - Math.min(1.0, ((double) compressionLevel) / Deflater.BEST_COMPRESSION);
                    }
                    new TarExporter(new SnappyFramedOutputStream(outputStream, SnappyFramedOutputStream.DEFAULT_BLOCK_SIZE, ratio));
                } catch (NoClassDefFoundError ex) {
                    LOG.trace("Native (JNI) snappy implementation not found.", ex);
                }
            }

            return new TarExporter(COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressionMethod, outputStream));
        } catch (CompressorException ex) {
            throw new IllegalArgumentException("Unknown compression format.");
        }
    }

    @Nonnull
    public AbstractExporter createExporter(@Nonnull File file, @Nonnull String compressionMethod, int compressionLevel)
            throws IOException {

        if ("deflate".equals(compressionMethod)) {
            return new JarExporter(file, compressionLevel);
        }

        OutputStream fileOut = new BufferedOutputStream(new FileOutputStream(file));
        return createExporter(fileOut, compressionMethod, compressionLevel);
    }
}
