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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.vault.fs.io.Archive;
import org.junit.Test;

public class PackageManagerImplTest {

    @Test
    public void testEntryTraversalIterator() throws IOException {
        EntryImpl root = new EntryImpl("", true);
        EntryImpl level1 = new EntryImpl("level1", true);
        root.add(level1);
        EntryImpl level2 = new EntryImpl("level2", true);
        level1.add(level2);
        EntryImpl file1 = new EntryImpl("file1", false);
        level2.add(file1);
        EntryImpl file2 = new EntryImpl("file2", false);
        level2.add(file2);
        EntryImpl level3 = new EntryImpl("level3", true);
        level2.add(level3);
        EntryImpl file3 = new EntryImpl("file3", false);
        level3.add(file3);
        EntryImpl file4 = new EntryImpl("file4", false);
        level2.add(file4);

        EntryImpl level4 = new EntryImpl("level4", true);
        root.add(level4);
        EntryImpl level5 = new EntryImpl("level5", true);
        level4.add(level5);
        EntryImpl level6 = new EntryImpl("level6", true);
        level5.add(level6);
        EntryImpl file5 = new EntryImpl("file5", false);
        level6.add(file5);

        EntryImpl file6 = new EntryImpl("file6", false);
        root.add(file6);

        Archive archive = mock(Archive.class);
        when(archive.getRoot()).thenReturn(root);
        PackageManagerImpl.EntryTraversalIterator allEntries = new PackageManagerImpl.EntryTraversalIterator(archive);
        List<String> nameExpectation = Arrays.asList(
                "level1",
                "level2",
                "file1",
                "file2",
                "level3",
                "file3",
                "file4",
                "level4",
                "level5",
                "level6",
                "file5",
                "file6"
        );
        List<String> pathExpectation = Arrays.asList(
                "level1",
                "level1/level2",
                "level1/level2/file1",
                "level1/level2/file2",
                "level1/level2/level3",
                "level1/level2/level3/file3",
                "level1/level2/file4",
                "level4",
                "level4/level5",
                "level4/level5/level6",
                "level4/level5/level6/file5",
                "file6"
        );
        int i = 0;

        while (allEntries.hasNext()) {
            PackageManagerImpl.EntryHolder entry = allEntries.next();
            assertEquals("Expectation failed at: " + i, nameExpectation.get(i), entry.entry.getName());
            assertEquals("Expectation failed at: " + i, pathExpectation.get(i), entry.relPath);
            i++;
        }

        assertEquals(i, nameExpectation.size());
    }

    @Test
    public void testAllEntries() throws IOException {
        EntryImpl root = new EntryImpl("", true);
        // add a file that somehow comes before MANIFEST.MF
        EntryImpl level1 = new EntryImpl("level1", true);
        root.add(level1);
        EntryImpl file1 = new EntryImpl("file1", false);
        level1.add(file1);
        // add the manifest
        EntryImpl metaInf = new EntryImpl("META-INF", true);
        EntryImpl manifest = new EntryImpl("MANIFEST.MF", false);
        metaInf.add(manifest);
        root.add(metaInf);
        // add other entries after the manifest
        EntryImpl level2 = new EntryImpl("level2", true);
        root.add(level2);
        EntryImpl file2 = new EntryImpl("file2", false);
        level2.add(file2);

        Archive archive = mock(Archive.class);
        when(archive.getRoot()).thenReturn(root);
        PackageManagerImpl.AllEntries allEntries = new PackageManagerImpl.AllEntries(archive);

        assertTrue("At least one entry expected.", allEntries.hasNext());
        assertEquals("MANIFEST.MF is expected to be the first entry.", JarFile.MANIFEST_NAME, allEntries.next().relPath);

        int i = 1;
        while (allEntries.hasNext()) {
            allEntries.next();
            i++;
        }

        assertEquals(6, i);
    }

    private class EntryImpl implements Archive.Entry {

        private EntryImpl parent = null;
        private final boolean isDirectory;
        private final String name;
        private final Map<String, Archive.Entry> entries = new LinkedHashMap<>();

        EntryImpl(String name, boolean directory) {
            this.name = name;
            this.isDirectory = directory;
        }

        void add(EntryImpl entry) {
            this.entries.put(entry.getName(), entry);
            entry.parent = this;
        }

        @Nonnull
        @Override public String getName() {
            return name;
        }

        @Nonnull
        private StringBuilder getPath(@Nonnull StringBuilder sb) {
            return parent == null ? sb : parent.getPath(sb).append('/').append(name);
        }

        @Override public boolean isDirectory() {
            return isDirectory;
        }

        @Nonnull @Override public Collection<? extends Archive.Entry> getChildren() {
            return entries.values();
        }

        @CheckForNull @Override public Archive.Entry getChild(@Nonnull String name) {
            return entries.get(name);
        }
    }
}
