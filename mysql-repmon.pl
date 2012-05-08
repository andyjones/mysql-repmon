#!/usr/bin/perl

# Script to investigate any bottlenecks in mysql replication

# BPS=binlog bytes added/processed per second
# so, if the binlog starts at 1000. One second later the binlog is at 2000. It has processed 1000bps

use strict;

use DBI;
use Term::ReadKey; # to calculate terminal width
use Getopt::Long;

# Mode state machine
my $MODE_COUNTER   = 1;
my $MODE_SLAVE_IO  = 2;
my $MODE_SLAVE_SQL = 3;

# Read mode constants
my $RM_RESET   = 0;
my $RM_NOECHO  = 2;
my $RM_NOBLKRD = 3; ## using 4 traps Ctrl-C :-(

my $starting_mode = $MODE_COUNTER;
my $slave_host = 'localhost';
my $username = 'root';
my $password = '';
my $prompt = 0;
my $poll_interval = 1;

Getopt::Long::GetOptions(
    'c|counter'                        => sub { $starting_mode = $MODE_COUNTER },
    'io|slaveio|slave-io|slave_io'     => sub { $starting_mode = $MODE_SLAVE_IO  },
    'sql|slavesql|slave-sql|slave_sql' => sub { $starting_mode = $MODE_SLAVE_SQL },
    'host|slave=s'                     => \$slave_host,
    'u|username=s'                     => \$username,
    'p|password=s'                     => \$password,
    't|poll|interval=i'                => \$poll_interval,
    'prompt'                           => \$prompt,
);

$|=1;

if ( $prompt ) {
    print "Password: ";
    ReadMode($RM_NOECHO);
    chomp($password = <STDIN>);
    ReadMode($RM_RESET);
    print "\n";
}

print "Connecting to slave $slave_host\n";
my $slave = dbi_connect($slave_host,$username,$password);

my $master_host = find_master($slave);

print "Connecting to master $master_host\n";
my $master = dbi_connect($master_host,$username,$password);

print "\n\n";

# trap control-c
$SIG{INT} = sub { exit(255); };

my $master_bps_avg = _Counter->new("MASTER Binlog increase bytes/sec");
my $slave_io_avg   = _Counter->new("SLAVE IO copied bytes/sec");
my $slave_sql_avg  = _Counter->new("SLAVE SQL processed bytes/sec");

my $mode = $starting_mode;
ReadMode($RM_NOBLKRD);

my %last_status;
my $displayed_counter_header = 0;
while ( 1 ) {
    # get the replication status
    my %status = replication_status($master,$slave);

    # Calculate the various bps
    my $master_bps = bps($last_status{MASTER_UPTO}, $last_status{TIME}, $status{MASTER_UPTO}, $status{TIME});

    my $slave_io_bps;
    if ( $status{SLAVE_IO_RUNNING} ) {
        $slave_io_bps = bps($last_status{SLAVE_IO_UPTO}, $last_status{TIME}, $status{SLAVE_IO_UPTO}, $status{TIME});
    }

    my $slave_sql_bps;
    if ( $status{SLAVE_SQL_RUNNING} ) {
        $slave_sql_bps = bps($last_status{SLAVE_SQL_UPTO}, $last_status{TIME}, $status{SLAVE_SQL_UPTO}, $status{TIME});
    }

    # Update counters
    if ( defined($master_bps) ) {
        $master_bps_avg->count($master_bps);
    }
    if ( defined($slave_io_bps) ) {
        $slave_io_avg->count($slave_io_bps);
    }
    if ( defined($slave_sql_bps) ) {
        $slave_sql_avg->count($slave_sql_bps);
    }

    # keep track of the last status
    %last_status = %status;

    # display info to screen
    my ($time, $date) = to_datetime($status{TIME});
    if ( $mode == $MODE_SLAVE_IO || $mode == $MODE_SLAVE_SQL ) {
        # display events as rows
        print "$time $date\n";
        print "  Master:   ", $master_bps,   " bytes/sec\n";
        print "  Slave IO: ", $slave_io_bps, " bytes/sec\n";
        print "  Slave SQL:", $slave_sql_bps, " bytes/sec\n";

        if ( $mode == $MODE_SLAVE_IO ) {
            print "  Next to copy:\t" . $status{SLAVE_IO_NEXT_STMT} . "\n\n";
        }
        if ( $mode == $MODE_SLAVE_SQL ) {
            print "  Next to execute:\t" . $status{SLAVE_SQL_NEXT_STMT} . "\n\n";
        }
    }
    else {
        # 1 row per loop, display in columns
        my $datetime_mask = "%-8.8s %-10.10s";
        if ( defined($master_bps) ) {
            my $row_mask = "$datetime_mask  %8.8s  %8.8s  %8.8s  %8.8s  ";
            if ( !$displayed_counter_header || $displayed_counter_header % terminal_height() == 0 ) {
                printf "$row_mask%s\n", qw/Time Date Master SlaveIO SlaveSQL Secs_Behind SQL_next_statement/;
            }

            $displayed_counter_header++;

            my $other_cols = sprintf $row_mask, $time, $date, $master_bps, $slave_io_bps, $slave_sql_bps, $status{BEHIND_MASTER};
            my $padding = 4;
            my $stmt_col_width = terminal_width() - length($other_cols) - $padding;
            print $other_cols,
                  shorten_statement($status{SLAVE_SQL_NEXT_STMT}, $stmt_col_width),
                  "\n";
        }
        elsif ( $status{REPLICATING} ) {
            printf "$datetime_mask: replication running\n", $time, $date;
        }
        else {
            printf "$datetime_mask: replication is not running\n", $time, $date;
        }
    }

    my $key = ReadKey($poll_interval);
    if ( $key eq 's' ) {
        $mode = $MODE_SLAVE_SQL;
    }
    elsif ( $key eq 'i' ) {
        $mode = $MODE_SLAVE_IO;
    }
    elsif ( $key eq 'c' ) {
        $mode = $MODE_COUNTER;
        $displayed_counter_header = 0;
    }
    
}

END {
    ReadMode($RM_RESET);
};

sub dbi_connect {
    my $host = shift;
    my $username = shift;
    my $password = shift;

    my $dbh = DBI->connect(dsn($host), $username, $password)
        or die "Unable to connect to $host: $DBI::errstr";
    return $dbh;
}

sub dsn {
    my $host = shift;
    my $db   = shift;

    return 'DBI:mysql:host='.$host;
}

sub find_master {
    my $slave = shift;

    my %status = slave_status($slave);

    if ( !$status{Master_Host} ) {
        die $slave->{Name} . " is not a slave\n";
    }

    return $status{Master_Host};
}

sub replication_status {
    my $master = shift;
    my $slave  = shift;

    my %slave_status = slave_status($slave);
    my %master_status = master_status($master);

    my %status = (
        TIME => time,
    );

    # what is the master doing?
    $status{MASTER_BINLOG} = $master_status{File};
    $status{MASTER_UPTO}   = $master_status{Position};

    # see if replication is running
    $status{SLAVE_IO_RUNNING}  = is_yes($slave_status{Slave_IO_Running});
    $status{SLAVE_SQL_RUNNING} = is_yes($slave_status{Slave_SQL_Running});
    $status{REPLICATING} = $status{SLAVE_IO_RUNNING} && $status{SLAVE_SQL_RUNNING};

    # what is the slave io thread up to?
    $status{SLAVE_IO_BINLOG} = $slave_status{Master_Log_File};
    $status{SLAVE_IO_UPTO}   = $slave_status{Read_Master_Log_Pos};
    if ( $status{SLAVE_IO_UPTO} ) {
        # There is only a next statement if the slave_io is behind the master
        if ( cmp_binlog_pos($status{MASTER_BINLOG}, $status{MASTER_UPTO}, $status{SLAVE_IO_BINLOG}, $status{SLAVE_IO_UPTO}) > 0 ) {
            $status{SLAVE_IO_NEXT_STMT} = next_binlog_event(
                $master,
                $status{SLAVE_IO_BINLOG},
                $status{SLAVE_IO_UPTO},
            );
        }
        else {
             $status{SLAVE_IO_NEXT_STMT} = "100% in sync";
        }
    }

    # what is the slave sql thread up to?
    $status{SLAVE_SQL_BINLOG} = $slave_status{Relay_Master_Log_File};
    $status{SLAVE_SQL_UPTO}   = $slave_status{Exec_Master_Log_Pos};
    if ( $status{SLAVE_SQL_UPTO} ) {
        # There is only a next statement if the slave_sql is behind the master
        if ( cmp_binlog_pos($status{MASTER_BINLOG}, $status{MASTER_UPTO}, $status{SLAVE_SQL_BINLOG}, $status{SLAVE_SQL_UPTO}) > 0 ) {
            $status{SLAVE_SQL_NEXT_STMT} = next_binlog_event(
                $master,
                $status{SLAVE_SQL_BINLOG},
                $status{SLAVE_SQL_UPTO},
            );
        }
        else {
            $status{SLAVE_SQL_NEXT_STMT} = "100% in sync";
        }
    }

    # How long ago did the last run query start on the master
    $status{BEHIND_MASTER} = $slave_status{Seconds_Behind_Master};

    return %status;
}

sub next_binlog_event {
    my $dbh =  shift;
    my $file = shift or die "Missing binlog file";
    my $pos  = shift;

    my %event = fetch_binlog_event($dbh, $file, $pos);
    my @events = summarise_event(%event);
    if ( $event{Event_type} eq 'Query' && $event{Info} eq 'BEGIN' ) {
        %event = fetch_binlog_event($dbh, $file, $pos, 1);
        push @events, summarise_event(%event);

        if ( $event{Event_type} eq 'Table_map' ) {
            %event = fetch_binlog_event($dbh, $file, $pos, 2);
            push @events, summarise_event(%event);

#            if ( $event{Event_type} eq 'Update_rows' ) {
#                %event = fetch_binlog_event($dbh, $file, $pos, 3);
#                push @events, summarise_event(%event);
#            }
        }
    }

    my $count = 0;
    return join("\n", map { ("\t" x ++$count) . $_ } @events);
}

sub summarise_event {
    my %event = @_;
    return join(':', $event{Event_type}, $event{Info});
}

sub fetch_binlog_event {
    my $dbh =  shift;
    my $file = shift or die "Missing binlog file";
    my $pos  = shift;
    my $offset = shift || 0;
    my $limit = 1;

    return sql_to_hash(
        $dbh, 'SHOW BINLOG EVENTS IN '.$dbh->quote($file)
                            . ' FROM '.$pos
                           . ' LIMIT '.$offset.', '.$limit,
    );
}

sub master_status {
    my $dbh = shift;

    return sql_to_hash($dbh, 'SHOW MASTER STATUS');
}

sub slave_status {
    my $dbh = shift;

    return sql_to_hash($dbh, 'SHOW SLAVE STATUS');
}

sub sql_to_hash {
    my $dbh = shift;
    my $sql = shift;

    my $sth = $dbh->prepare($sql)
        or die $dbh->errstr();

    $sth->execute()
        or die $sth->errstr();

    my $data_ref = $sth->fetchrow_hashref();
    $sth->finish();

    if ( !$data_ref ) {
        warn $dbh->{Name}.": $sql failed\n";
        return ();
    }

    return %$data_ref;
}

##  MISC UTILS
# Summarises output from fetch_bindlog_event and 
sub shorten_statement {
    my $stmt = shift;
    my $max_length = shift;

    # collapse all spaces
    $stmt =~ s/\n\s*/../g;
    $stmt =~ s/^\s+//; $stmt =~ s/\s+$//;

    my $length_ok_ref = sub { return length($stmt) <= $max_length; };
    if ( $length_ok_ref->() ) {
        return $stmt;
    }

    # remove transaction BEGIN/COMMIT as they aren't that interesting
    $stmt =~ s/^Query:BEGIN\.*//;

    # remove the Table_map table_id bit as it also contains
    # the table name which is more useful
    $stmt =~ s/^Table_map:table_id:\s+\d+\s+//;

    if ( $stmt =~ s/^\(([^\)]+)\)\.*/$1: / ) {
        # got a table name, we don't need the table_id stuff
        $stmt =~ s/table_id:\s+\d+\s+//;
        $stmt =~ s/:flags: STMT_END_F//;
    }

    if ( $length_ok_ref->() ) {
        return $stmt;
    }

    return substr($stmt, 0, $max_length);
}

# returns 1 if $a is ahead of $b
# returns 0 if $a is the same position as $b
# ruturns -1 if $a is behind $b
sub cmp_binlog_pos {
    my $a_binlog = shift;
    my $a_pos    = shift;
    my $b_binlog = shift;
    my $b_pos    = shift;

    if ( $a_binlog eq $b_binlog ) {
        return $a_pos <=> $b_pos;
    }

    my ($a_binlog_count) = ( $a_binlog =~ m/\.(.+)$/ );
    my ($b_binlog_count) = ( $b_binlog =~ m/\.(.+)$/ );

    return $a_binlog_count <=> $b_binlog_count;
}

# calculates the difference between two positions in a binary log per second
sub bps {
    my $start_pos = shift;
    my $start_time = shift
        or return;
    my $end_pos = shift;
    my $end_time = shift;

    if ( $end_pos == $start_pos || $start_time == $end_time) {
        return 0;
    }

    my $elapsed = $end_time - $start_time;
    my $delta_pos = $end_pos - $start_pos;

    return sprintf("%.0f", $delta_pos / $elapsed);
}

sub is_yes {
    return $_[0] =~ m/yes/i;
}

# Functions to make things look pretty
sub terminal_width {
    my ($width, $height, $w_px, $h_px) = GetTerminalSize();
    return $width;
}

sub terminal_height {
    my ($width, $height, $w_px, $h_px) = GetTerminalSize();
    return $height;
}

# returns an array containing the $time and $date
sub to_datetime {
    my $epoch = shift || time;
    my @dt = localtime($epoch);

    return (
        sprintf("%02d:%02d:%02d",$dt[2], $dt[1], $dt[0]),
        sprintf("%02d-%02d-%04d", $dt[3], $dt[4]+1, $dt[5]+1900),
    );
}

## MISC UTILS OVER


# package to keep track of the average value and print them out automatically when they go out of scope
package _Counter;

use strict;

sub new {
    my $class = shift;
    my $title = shift;

    return bless {
        title => $title,
        total => 0,
        count => 0,
    }, $class;
}

sub count {
    $_[0]->{count}++;
    return $_[0]->{total} += $_[1];
} 

sub DESTROY {
    my $self = shift;
    
    if ( $self->{count} ) {
        my $count = $self->{count};
        my $total = $self->{total};
        my $avg   = $total ? $total / $count : '0';

        printf "%s: average %.2f from %d counts\n", $self->{title}, $avg, $count;
    }
}

1;
