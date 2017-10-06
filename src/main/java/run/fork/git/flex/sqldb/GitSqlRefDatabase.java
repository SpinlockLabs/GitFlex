package run.fork.git.flex.sqldb;

import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GitSqlRefDatabase extends RefDatabase {
    private final GitSqlRepository parent;

    public GitSqlRefDatabase(GitSqlRepository parent) {
        this.parent = parent;
    }

    @Override
    public void create() throws IOException {
        try {
            Statement creates = parent.getConnection().createStatement();

            creates.execute("DROP TABLE IF EXISTS `git.refs`");

            creates.execute(
                    "CREATE TABLE `git.refs` (\n" +
                            "  `name` VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Reference Name' PRIMARY KEY,\n" +
                            "  `symbolic` BOOLEAN NOT NULL COMMENT 'Indicates if the Reference is Symbolic',\n" +
                            "  `target` VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Reference Target'\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT 'Git References';"
            );

            creates.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isNameConflicting(String name) throws IOException {
        try {
            PreparedStatement statement = parent.getConnection().prepareStatement(
                    "SELECT `name` FROM `git.refs` WHERE `name` = ?"
            );
            statement.setString(1, name);
            ResultSet results = statement.executeQuery();
            boolean conflicts = false;
            if (results.next()) {
                conflicts = true;
            }
            statement.close();
            return conflicts;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public RefUpdate newUpdate(String name, boolean detach) throws IOException {
        Ref ref = getRef(name);

        if (ref == null) {
            ref = new ObjectIdRef.Unpeeled(Ref.Storage.NEW, name, null);
        }

        return new SqlRefUpdate(ref);
    }

    @Override
    public RefRename newRename(String fromName, String toName) throws IOException {
        return null;
    }

    @Override
    public Ref getRef(String name) throws IOException {
        try {
            PreparedStatement statement = parent.getConnection().prepareStatement(
                    "SELECT `target`, `symbolic` FROM `git.refs` WHERE `name` = ?"
            );
            statement.setString(1, name);
            ResultSet results = statement.executeQuery();

            if (!results.next()) {
                statement.close();
                return null;
            }

            boolean symbolic = results.getBoolean("symbolic");
            String target = results.getString("target");
            statement.close();

            if (symbolic) {
                Ref targetRef = getRef(target);
                if (targetRef == null) {
                    throw new IOException("Failed to link " + name + " to " + target + ": Target not found.");
                }
                return new SymbolicRef(name, targetRef);
            } else {
                return new ObjectIdRef(Ref.Storage.LOOSE, name, ObjectId.fromString(target)) {
                    @Override
                    public ObjectId getPeeledObjectId() {
                        return null;
                    }

                    @Override
                    public boolean isPeeled() {
                        return false;
                    }
                };
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Map<String, Ref> getRefs(String prefix) throws IOException {
        try {
            PreparedStatement statement = parent.getConnection().prepareStatement(
                    "SELECT `name` FROM `git.refs` WHERE `name` LIKE ?"
            );
            statement.setString(1, prefix + "%");
            ResultSet results = statement.executeQuery();

            if (!results.next()) {
                statement.close();
                return null;
            }

            HashMap<String, Ref> refs = new HashMap<>();
            do {
                String name = results.getString("name");
                refs.put(name, getRef(name));
            } while(results.next());
            statement.close();
            return refs;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<Ref> getAdditionalRefs() throws IOException {
        return null;
    }

    @Override
    public Ref peel(Ref ref) throws IOException {
        return null;
    }

    @Override
    public void close() {
        try {
            parent.getConnection().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public class SqlRefUpdate extends RefUpdate {
        public SqlRefUpdate(final Ref ref) {
            super(ref);
        }

        @Override
        protected RefDatabase getRefDatabase() {
            return GitSqlRefDatabase.this;
        }

        @Override
        protected GitSqlRepository getRepository() {
            return parent;
        }

        @Override
        protected boolean tryLock(boolean deref) throws IOException {
            return true;
        }

        @Override
        protected void unlock() {
        }

        @Override
        protected Result doUpdate(Result status) throws IOException {
            try {
                PreparedStatement statement = getRepository().getConnection().prepareStatement(
                        "INSERT INTO `git.refs` (`target`, `symbolic`, `name`) VALUES (?, ?, ?)" +
                                " ON DUPLICATE KEY UPDATE `target` = ?, `symbolic` = ?"
                );

                statement.setString(1, getNewObjectId().name());
                statement.setBoolean(2, getRef().isSymbolic());
                statement.setString(3, getRef().getName());
                statement.setString(4, getNewObjectId().name());
                statement.setBoolean(5, getRef().isSymbolic());

                if (statement.executeUpdate() == 0) {
                    return Result.REJECTED;
                }
                return status;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected Result doDelete(Result status) throws IOException {
            try {
                PreparedStatement statement = getRepository().getConnection().prepareStatement(
                        "DELETE FROM `git.refs` WHERE `name` = ?"
                );

                statement.setString(1, getRef().getName());
                if (statement.executeUpdate() == 0) {
                    return Result.REJECTED;
                }
                return status;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected Result doLink(String target) throws IOException {
            try {
                PreparedStatement statement = getRepository().getConnection().prepareStatement(
                        "INSERT INTO `git.refs` (`name`, `symbolic`, `target`) VALUES (?, ?, ?)"
                );

                statement.setString(1, getRef().getName());
                statement.setBoolean(2, true);
                statement.setString(3, target);
                if (!statement.execute()) {
                    return Result.REJECTED;
                }
                return Result.NEW;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
