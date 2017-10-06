package run.fork.git.flex.sqldb;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ImportMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: git-sql-import <url> <username> <password>");
            System.exit(1);
        }

        System.out.println("Using connection " + args[0]);

        Connection connection = DriverManager.getConnection(
                args[0],
                args[1],
                args[2]
        );

        GitSqlRepository repo = new GitSqlRepository(connection, new BaseRepositoryBuilder());
        repo.create(true);
        ObjectInserter inserter = repo.newObjectInserter();

        {
            Git git = Git.open(new File("."));
            Repository tmp = git.getRepository();
            ObjectReader reader = tmp.getObjectDatabase().newReader();
            Set<String> objects = new HashSet<>();
            for (RevCommit commit : git.log().all().call()) {
                RevTree tree = commit.getTree();
                TreeWalk walk = new TreeWalk(tmp);
                walk.addTree(tree);
                walk.setRecursive(true);

                while (walk.next()) {
                    objects.add(walk.getObjectId(0).name());
                }

                objects.add(tree.name());
                objects.add(commit.name());
            }

            System.out.println("" + objects.size() + " objects to import...");
            int counter = 0;
            for (String id : objects) {
                counter++;
                System.out.println("Importing Object " + id);

                ObjectLoader loader = reader.open(ObjectId.fromString(id));
                byte[] bytes = loader.getBytes();
                int type = loader.getType();

                ObjectId result = inserter.insert(type, bytes);

                if (!id.equals(result.name())) {
                    System.out.println("Differing Object (" + id + " => " + result.name() + ")");
                }

                if ((counter % 100) == 0) {
                    System.out.println("Commit (" + counter + " of " + objects.size() + " objects)");
                    inserter.flush();
                }
            }
            System.out.println("Commit (" + counter + " of " + objects.size() + " objects)");
            inserter.flush();

            objects.clear();

            Map<String, Ref> refs = tmp.getAllRefs();
            for (Ref ref : refs.values()) {
                if (ref.getName().equals("HEAD")) {
                    continue;
                }

                System.out.println("Importing Reference " + ref.getName());
                RefUpdate upd = repo.updateRef(ref.getName());
                if (ref.isSymbolic()) {
                    if (upd.link(ref.getTarget().getName()) != RefUpdate.Result.NEW) {
                        System.out.println("Failed to import reference " + ref.getName());
                    }
                } else {
                    upd.setNewObjectId(ref.getObjectId());
                    RefUpdate.Result result = upd.update();

                    if (result != RefUpdate.Result.NEW) {
                        System.out.println("Failed to import reference " + ref.getName());
                    }
                }
            }

            inserter.close();
            tmp.close();
        }
    }
}
