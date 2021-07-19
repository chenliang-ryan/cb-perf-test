# Couchbase Server Performance Test Utility

## Usage:
perftest.exe [Target] [Number of Populating Threads] [Number of Writing Threads] [Number Of Reading Threads] [Dataset Size] [Number Of Operations] [Cluster Address] [Database Name] [User Name] [Password]

## Options:
| Argument                     | Remarks                                             |
|------------------------------|-----------------------------------------------------|
| Target                       | [mssql \| cbkv \| cbquery]                          |
| Number of Populating Threads | 0 - 200                                             |
| Number of Writing Threads    | 0 - 200                                             |
| Number of Reading Threads    | 0 - 200                                             |
| Dataset Size                 | Number of records to be used in the test.           |
| Number of Operations         | Number of operations to be executed in each thread. |
| Cluster Address              | IP address of the target cluster.                   |
| Database Name                | Database name.                                      |
| User Name                    | Username.                                           |
| Password                     | Password.                                           |
| Show Error                   | [0 | 1]                                             |

## Example:
* Run a performance test against Couchbase Server using KV API, using 100 threads to populate 2M document into the bucket "poc", then using 100 update threads and 100 query threads to perform 20K operations concurrently. Login is pocAdmin and password is password.
```console
.\perftest.exe cbkv 50 50 50 2000000 10000 cbs66sn poc pocAdmin password 0
```

* Run a performance test against Couchbase Server using Query API, using 100 threads to populate 2M document into the bucket "poc", then using 100 update threads and 100 query threads to perform 20K operations concurrently. Login is pocAdmin and password is password.
```console
.\perftest.exe cbkv 50 50 50 2000000 10000 cbs66sn poc pocAdmin password 0
```

* Run a performance test against MSSQL, using 100 threads to populate 2M records the database "poc", then using 100 update threads and 100 query threads to perform 20K operations concurrently. Login is pocAdmin and password is password.
```console
.\perftest.exe mssql 100 100 100 2000000 20000 mssqlsrv poc pocAdmin password 0
```

## Provision
### Couchbase Server
Follow the steps below to provision the Couchbase Server:
* Make sure the Couchbase Server cluster was provisioned with Data, Index, Query and Analysis Service.
* Create a bucket.
* Create primary index on the bucket.
* Create a login with at least "Application Access" role of the bucket.
* Create a dataset (assuming the bucket name is poc).
```sql
CREATE DATASET PerfStats ON poc WHERE docType = 'Stats'
```

### SQL Server
Follow the steps below to provision the SQL Server:
* Create a database
* Create a login with dbo role on the new database
* Create tables with the provided SQL scripts


## Get benchmark outcome
### Couchbase Server
Run following query in Analytics Service
```sql
SELECT opType AS OperationType, AVG(response) AS ResponseTime, COUNT(response) AS NumberOfOperations FROM PerfStats GROUP BY opType
```

### SQL Server
Run following query 
```sql
SELECT OpType AS OperationType, Avg(ResponseTime) AS ResponseTime, COUNT(ResponseTime) AS NumberOfOperations FROM PerfStats GROUP BY OpType
```
