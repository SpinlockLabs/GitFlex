package run.fork.git.flex.sqldb;

import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.internal.storage.file.ObjectDirectoryPackParser;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.util.sha1.SHA1;

import javax.swing.plaf.nimbus.State;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class GitSqlObjectDatabase extends ObjectDatabase {
    private final GitSqlRepository parent;

    public GitSqlObjectDatabase(GitSqlRepository parent) {
        this.parent = parent;
    }

    @Override
    public void create() throws IOException {
        try {
            try {
                Statement drop = parent.getConnection().createStatement();
                drop.execute("DROP TABLE `git.objects`");
            } catch (SQLException ignored) {
            }

            Statement creates = parent.getConnection().createStatement();
            creates.execute(
              "CREATE TABLE `git.objects` (\n" +
                      "  `hash` VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Object Hash' PRIMARY KEY,\n" +
                      "  `type` TINYINT(4) NOT NULL COMMENT 'Object Type',\n" +
                      "  `content` LONGBLOB NOT NULL COMMENT 'Object Content'\n" +
                      ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Git Objects'"
            );
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists() {
        try {
            DatabaseMetaData dbm = parent.getConnection().getMetaData();
            ResultSet tables = dbm.getTables(null, null, "git.objects", null);
            if (!tables.next()) {
                tables.close();
                return false;
            }

            tables.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectInserter newInserter() {
        return new SqlObjectInserter();
    }

    @Override
    public ObjectReader newReader() {
        return new SqlObjectReader();
    }

    @Override
    public void close() {
        try {
            parent.getConnection().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public class SqlObjectReader extends ObjectReader {
        @Override
        public ObjectReader newReader() {
            return new SqlObjectReader();
        }

        @Override
        public Collection<ObjectId> resolve(AbbreviatedObjectId id) throws IOException {
            try {
                PreparedStatement statement = parent.getConnection().prepareStatement(
                        "SELECT `hash` FROM `git.objects` WHERE `hash` LIKE ?"
                );
                statement.setString(1, id.name() + "%");
                ArrayList<ObjectId> ids = new ArrayList<>();
                ResultSet results = statement.executeQuery();

                while (results.next()) {
                    String oid = results.getString(0);
                    ids.add(ObjectId.fromString(oid));
                }
                return ids;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public ObjectLoader open(AnyObjectId objectId, int typeHint) throws IOException {
            SqlObjectLoader loader = new SqlObjectLoader(objectId, typeHint);
            loader.loadCache();
            return loader;
        }

        @Override
        public Set<ObjectId> getShallowCommits() throws IOException {
            return null;
        }

        @Override
        public void close() {
        }
    }

    public class SqlObjectLoader extends ObjectLoader {
        private final AnyObjectId objectId;
        private final int typeHint;

        private int cachedType;
        private long cachedSize;
        private byte[] cachedBlobData;

        private boolean cacheLoaded = false;

        public SqlObjectLoader(AnyObjectId objectId, int typeHint) {
            this.objectId = objectId;
            this.typeHint = typeHint;

            cachedBlobData = null;
        }

        private void loadCache() throws IOException {
            try {
                PreparedStatement statement = parent.getConnection().prepareStatement(
                        "SELECT `type`, LENGTH(`content`) AS size FROM `git.objects` WHERE `hash` = ?"
                );
                statement.setString(1, objectId.name());
                ResultSet results = statement.executeQuery();
                if (!results.next()) {
                    throw new MissingObjectException(objectId.toObjectId(), typeHint);
                }

                cachedSize = results.getLong("size");
                cachedType = results.getInt("type");
                cacheLoaded = true;
                statement.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int getType() {
            if (!cacheLoaded) {
                try {
                    loadCache();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return cachedType;
        }

        @Override
        public long getSize() {
            if (!cacheLoaded) {
                try {
                    loadCache();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return cachedSize;
        }

        @Override
        public byte[] getCachedBytes() throws LargeObjectException {
            return cachedBlobData;
        }

        @Override
        public ObjectStream openStream() throws IOException {
            if (cachedBlobData != null) {
                return new ObjectStream.SmallStream(getType(), getCachedBytes());
            }

            try {
                PreparedStatement statement = parent.getConnection().prepareStatement(
                        "SELECT `type`, `content` AS size FROM `git.objects` WHERE `hash` = ?"
                );
                statement.setString(1, objectId.name());
                ResultSet results = statement.executeQuery();
                if (!results.next()) {
                    throw new MissingObjectException(objectId.toObjectId(), typeHint);
                }

                Blob blob = results.getBlob("content");
                byte[] bytes = blob.getBytes(0, (int) blob.length());
                blob.free();
                cacheLoaded = true;
                cachedSize = bytes.length;
                cachedBlobData = bytes;
                cachedType = results.getInt("type");
                statement.close();
                return new ObjectStream.SmallStream(cachedType, bytes);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    public class SqlObjectInserter extends ObjectInserter {
        private PreparedStatement statement;

        void writeHeader(OutputStream out, final int type, long len)
                throws IOException {
            out.write(Constants.encodedTypeString(type));
            out.write((byte) ' ');
            out.write(Constants.encodeASCII(len));
            out.write((byte) 0);
        }

        @Override
        public ObjectId insert(int objectType, long length, InputStream in) throws IOException {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                SHA1 sha = digest();
                SHA1OutputStream shaOut = new SHA1OutputStream(out, sha);

                writeHeader(shaOut, objectType, length);
                {
                    int nRead;
                    byte[] data = buffer();

                    while ((nRead = in.read(data, 0, data.length)) != -1) {
                        shaOut.write(data, 0, nRead);
                    }
                    shaOut.flush();
                }

                byte[] bytes = out.toByteArray();
                ObjectId id = sha.toObjectId();

                if (statement == null) {
                    statement = parent.getConnection().prepareStatement(
                            "INSERT INTO `git.objects` (`hash`, `type`, `content`) VALUES (?, ?, ?)"
                    );
                }

                statement.setString(1, id.name());
                statement.setInt(2, objectType);
                statement.setBlob(3, new ByteArrayInputStream(bytes));
                statement.addBatch();
                statement.clearParameters();
                return id;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public PackParser newPackParser(InputStream in) throws IOException {
            return null;
        }

        @Override
        public ObjectReader newReader() {
            return new SqlObjectReader();
        }

        @Override
        public void flush() throws IOException {
            if (statement == null) {
                return;
            }

            try {
                statement.executeBatch();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
            if (statement == null) {
                return;
            }

            try {
                statement.executeBatch();
                statement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SHA1OutputStream extends FilterOutputStream {
        private final SHA1 md;

        SHA1OutputStream(OutputStream out, SHA1 md) {
            super(out);
            this.md = md;
        }

        @Override
        public void write(int b) throws IOException {
            md.update((byte) b);
            out.write(b);
        }

        @Override
        public void write(byte[] in, int p, int n) throws IOException {
            md.update(in, p, n);
            out.write(in, p, n);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }
    }
}
