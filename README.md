mysql-repmon
============

investigate bottlenecks in mysql replication

Prerequisites
=============

 1. check perl is installed
 2. install CPAN libraries DBI, Getopt::Long and Term::ReadKey

Basic Usage
===========

./mysql-repmon.pl --host=SLAVE.MYSQL.COM -u USER --prompt

12:22:29 08-05-2012: replication running
Time     Date          Master   SlaveIO  SlaveSQL  Secs_Beh  SQL_next_statement
12:22:30 08-05-2012     33359     33843       923    146893  apli2.source_metri
12:22:31 08-05-2012     34397     33573      4906    146894  apli2.source_metri
12:22:32 08-05-2012    241189    242013       927    146895  apli2.source_metri
12:22:33 08-05-2012     26368     26368       923    146896  apli2.source_metri
12:22:34 08-05-2012     31152     31152      2964    146897  apli2.source_metri
12:22:35 08-05-2012     21434     21434      1505    146898  apli2.source_metri

Exit with Control-C at any time.

The Master column shows the rate at which the master is writing the the binlog.

The SlaveIO column shows the rate at which the slave is copying the binlog from
the master to the slave. It is the bottleneck if the SlaveIO is less than the Master

The SlaveSQL column shows the rate at which the slave is applying the binlog. In the example above it is the bottleneck.

Secs_Beh column shows how far the slave is behind the master in seconds.

The SQL_next_statement is the best guess at the next statement that the slave will
apply. If you are using row-based replication (ie. binlog-format=ROW or MIXED)
this information will only tell you what table is being updated.

If you are using statement-based replication, this column may provide the exact
SQL that is next to run.
