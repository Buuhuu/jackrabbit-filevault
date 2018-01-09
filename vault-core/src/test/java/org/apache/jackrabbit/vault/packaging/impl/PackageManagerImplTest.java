package org.apache.jackrabbit.vault.packaging.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.vault.fs.io.Archive;
import org.junit.Test;

public class PackageManagerImplTest {

    @Test
    public void testAllEntries() throws IOException {
        EntryImpl root = new EntryImpl("", true);
        EntryImpl level1 = new EntryImpl("level1", true);
        root.add(level1);
        EntryImpl level2 = new EntryImpl("level2", true);
        level1.add(level2);
        EntryImpl file1 = new EntryImpl("file1", false);
        level2.add(file1);
        EntryImpl file2 = new EntryImpl("file2", false);
        level2.add(file2);
        EntryImpl file3 = new EntryImpl("file3", true);
        level2.add(file3);

        EntryImpl level3 = new EntryImpl("level3", true);
        root.add(level3);
        EntryImpl level4 = new EntryImpl("level4", true);
        level3.add(level4);
        EntryImpl level5 = new EntryImpl("level5", true);
        level4.add(level5);
        EntryImpl file4 = new EntryImpl("file4", true);
        level5.add(file4);

        Archive archive = mock(Archive.class);
        when(archive.getRoot()).thenReturn(root);
        Iterator<Archive.Entry> allEntries = new PackageManagerImpl.AllEntries(archive);
        List<String> expectation = Arrays.asList("level1", "level2", "file1", "file2", "file3", "level3", "level4", "level5", "file4");
        int i = 0;

        while (allEntries.hasNext()) {
            assertEquals("Expectation failed at: " + i, expectation.get(i), allEntries.next().getName());
            i++;
        }

        assertEquals(i, expectation.size());
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
        public String getPath() {
            return getPath(new StringBuilder()).toString();
        }

        @Nonnull
        public String getRelPath() {
            return getPath(new StringBuilder()).substring(1);
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
