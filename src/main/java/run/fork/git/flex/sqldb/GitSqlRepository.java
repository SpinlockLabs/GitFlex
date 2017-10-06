package run.fork.git.flex.sqldb;

import org.eclipse.jgit.attributes.AttributesNodeProvider;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class GitSqlRepository extends Repository {
    private final Connection connection;
    private final GitSqlObjectDatabase objectDatabase;
    private final GitSqlRefDatabase refDatabase;

    public GitSqlRepository(Connection connection, BaseRepositoryBuilder builder) {
        super(builder);

        this.connection = connection;
        this.objectDatabase = new GitSqlObjectDatabase(this);
        this.refDatabase = new GitSqlRefDatabase(this);
    }

    @Override
    public void create(boolean bare) throws IOException {
        if (!bare) {
            throw new IOException("Non-bare Git repositories are not supported.");
        }

        objectDatabase.create();
        refDatabase.create();

        RefUpdate head = updateRef(Constants.HEAD);
        head.disableRefLog();
        head.link(Constants.R_HEADS + Constants.MASTER);
    }

    @Override
    public ObjectDatabase getObjectDatabase() {
        return objectDatabase;
    }

    @Override
    public RefDatabase getRefDatabase() {
        return refDatabase;
    }

    @Override
    public StoredConfig getConfig() {
        return new StoredConfig() {
            @Override
            public void load() throws IOException, ConfigInvalidException {
            }

            @Override
            public void save() throws IOException {
            }
        };
    }

    @Override
    public AttributesNodeProvider createAttributesNodeProvider() {
        return null;
    }

    @Override
    public void scanForRepoChanges() throws IOException {
    }

    @Override
    public void notifyIndexChanged() {
    }

    @Override
    public ReflogReader getReflogReader(String refName) throws IOException {
        return null;
    }

    public Connection getConnection() {
        return connection;
    }
}
