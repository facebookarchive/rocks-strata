# rocks-strata: A Framework for Managing Incremental Backups with the RocksDB Storage Engine

[![Build Status](https://secure.travis-ci.org/facebookgo/rocks-strata.png?branch=master)](http://travis-ci.org/facebookgo/rocks-strata)

rocks-strata is a framework for managing incremental backups of databases that use the RocksDB storage engine. Current drivers support MongoDB with the RocksDB storage engine ("MongoRocks") and use Amazon S3 for remote storage. rocks-strata also includes a tool to query backups from a MongoDB shell without doing a restore.

rocks-strata takes advantage of RocksDB's architecture, which makes cheap incremental backups possible. It can be extended to work with different database management systems and different storage environments.

rocks-strata has been running in production at Parse since about August 13, 2015. Here's the blog post: http://blog.parse.com/learn/engineering/strata-open-source-library-for-efficient-mongodb-backups/

For more information about running your Mongo database with the RocksDB storage engine, see https://github.com/mongodb-partners/mongo-rocks.

## Supported Platforms

rocks-strata should work on most Linux distributions. It has been tested on Ubuntu.

Scripts under `examples/` might use tools that are not available on all distributions.

## Getting Started

If you'd like to use rocks-strata with MongoRocks (which is the main focus of this project), you'll need to set up MongoRocks first: https://github.com/mongodb-partners/mongo-rocks.

You'll need to have Go installed to build rocks-strata. See https://golang.org/doc/install.

Get rocks-strata and its dependencies with `go get github.com/facebookgo/rocks-strata/strata`.

To back up up to S3, first build the driver:
```
cd rocks-strata/strata/cmd/mongo/lreplica_drivers/strata
go build
```

Check out `./strata --help`. To see more about a subcommand, such as `gc`, do `./strata gc --help`.

### Try a one-off backup and restore

This driver looks for the environment variables `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID`. So, do something like the following. Note: This example will write your AWS credentials to your bash history.
```
echo "export AWS_SECRET_ACCESS_KEY=REPLACEME" >> aws_credentials
echo "export AWS_ACCESS_KEY_ID=REPLACEME" >> aws_credentials
source aws_credentials
```

To try a one-off backup, create a test database.
```
mkdir tmpdb
mongod --dbpath=/absolute/path/to/tmpdb --storageEngine=rocksdb
# Change shells
mongo --eval 'db.testc.insert( { _id: "cheetah" } )'
```

Then, while mongod is still running, perform a backup! The values of `--bucket`, `--bucket-prefix`, and `--replica-id` below are just suggestions.
```
cd rocks-strata/strata/cmd/mongo/lreplica_drivers/strata
./strata --bucket=all_backups --bucket-prefix=mongo-rocks backup --replica-id=one-off-test
```
If you used a non-default port to start mongod, you'll need to specify it with `./strata --port=`.

Run `./strata --bucket=all_backups --bucket-prefix=mongo-rocks show backups --replica-id=one-off-test` to list the backups on S3. You should see something like this:
```
ID   data                      num files   size (GB)   incremental files   incremental size   duration
0    2015-09-02 21:11:20 UTC   4           0.000005    4                   0.000005           187.929573ms
```

Now, suppose your database server burns to the ground:
```
# kill mongod
rm -rf tmpdb
```

Restore and restart mongod:
```
./strata --bucket=all_backups --bucket-prefix=mongo-rocks restore --replica-id=one-off-test --backup-id=0 --target-path=/absolute/path/to/tmpdb
mongod --dbpath=/absolute/path/to/tmpdb --storageEngine=rocksdb
```

In another shell, launch the MongoDB shell (`mongo`) and enter `db.testc.find()`. You should see the record `{ "_id" : "cheetah" }`.

### Query backups from an extended MongoDB shell

Inspecting backups is almost as easy as inspecting live databases. First, build an extended MongoDB shell:
```
cd rocks-strata/strata/cmd/mongo/lreplica_drivers/mongoq
go build
```

This is a wrapper that adds new commands to the MongoDB shell. Check out `./mongoq --help`.

To use this, you need to mount part of S3 as a read-only file system in user space. Set up a FUSE implementation such as yas3fs: https://github.com/danilop/yas3fs.

With yas3fs and the example bucket/prefix names from the previous section, run `yas3fs s3://all_backups/mongo-rocks /path/to/desired/mountpoint --mkdir --no-metadata --read-only -f -d`. You'll still need the AWS credentials environment variables from the previous section. If you need to use sudo, use `sudo -E` to preserve the environment variables.

Congrats, you should be set up to query arbitrary backups from a command-line shell! To make things a little more interesting, let's do another one-off backup first.
```
mongod --dbpath=/absolute/path/to/tmpdb --storageEngine=rocksdb
# Change shells
mongo --eval 'db.testc.insert( { _id: "tiger" } )'
cd rocks-strata/strata/cmd/mongo/lreplica_drivers/strata
./strata --bucket=all_backups --bucket-prefix=mongo-rocks backup --replica-id=one-off-test
```

Now, launch the extended MongoDB shell with `./mongoq -b=all_backups -p=mongo-rocks -m=/path/to/desired/mountpoint`. If `mongod` and `mongo` aren't on your path, you'll need to use the `--mongod-path` and `--mongo-path` arguments as well. You should be able to go through a session like the one below. Some output has been replaced by "..." for brevity.
```
> help
...
> status
Current replica ID: ?
Current backup ID: ?
> lr
Replicas with backups in remote storage:
one-off-test
> cr one-off-test
Replica ID changing to one-off-test
Database state is undefined until change_backup [cb] is run
> lb
ID   data                      num files   size (GB)   incremental files   incremental size   duration
0    2015-09-02 21:11:20 UTC   4           0.000005    4                   0.000005           187.929573ms   
1    2015-09-03 16:00:06 UTC   6           0.000009    4                   0.000004           299.54062ms    
> cb 0
...
> show dbs
local  0.000GB
test   0.000GB
> db.testc.find()
{ "_id" : "cheetah" }
> cb 1
...
> db.testc.find()
{ "_id" : "cheetah" }
{ "_id" : "tiger" }
> exit
```

### Set up regular backups

See `examples/backups` for ideas.

Note that for a given replica ID, at most one operation that is capable of writing to storage (backup, delete, gc) should be running at any one time. That is why `examples/backups` uses start-stop-daemon. It is safe to restore while a write-capable operation is underway, as long as the backup being restored from isn't deleted while the restore is in progress. Operations that use one replica ID do not affect operations that use a different replica ID.

To avoid impacting performance in production, you may wish to dedicate a hidden MongoDB replica to performing backups.

### Set up monitoring

See `examples/monitoring` for ideas

### Using password auth

If your mongo distribution has authentication enabled, you must specify a username and password when running the backup command. This user needs access to run the *getCmdLineOpts* and *setParameter* commands on the admin database. To configure a backup user with minimal access needed to run strata backups, you can define a custom role with only these permissions:

```
> db.createRole({role: "rocksBackupRole", privileges: [ { resource: {cluster: true}, actions: ["getCmdLineOpts", "setParameter"]}], roles: [] })
{
        "role" : "rocksBackupRole",
        "privileges" : [
                {
                        "resource" : {
                                "cluster" : true
                        },
                        "actions" : [
                                "getCmdLineOpts",
                                "setParameter"
                        ]
                }
        ],
        "roles" : [ ]
}
> db.createUser({user: "backupUser", pwd: "abc123", roles: [ {role: "rocksBackupRole", db: "admin"} ]})
Successfully added user: {
        "user" : "backupUser",
        "roles" : [
                {
                        "role" : "rocksBackupRole",
                        "db" : "admin"
                }
        ]
}
```

## Extending Strata

You can extend rocks-strata by implementing the Storage and Replica interfaces, defined in `manager.go`. And, you will probably want to implement a driver similar to `rocks-strata/strata/cmd/mongo/lreplica_drivers/strata/main.go`.
