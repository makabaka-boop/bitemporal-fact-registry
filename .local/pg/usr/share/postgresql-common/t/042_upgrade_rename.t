# Test in-version upgrading (usually used after catalog version bumps)

use strict;

use lib 't';
use TestLib;
use PgCommon;

use Test::More tests => 15 * @MAJORS;

foreach my $v (@MAJORS) {
  SKIP: {
    skip "pg_upgrade not supported on $v", 15 if ($v < 9.2);
    note "PostgreSQL $v";

    program_ok 0, "pg_createcluster $v main --start", 0;
    program_ok 0, "pg_upgradecluster -m upgrade --old-bindir=$PgCommon::binroot$v/bin -v $v --rename upgr $v main", 0;
    like_program_out 0, "pg_lsclusters -h", 0, qr/$v main 5433 down.*\n$v upgr 5432 online/;

    program_ok 0, "pg_dropcluster $v main --stop", 0;
    program_ok 0, "pg_dropcluster $v upgr --stop", 0;
    is ((system "rm -rf /var/log/postgresql/pg_upgradecluster-$v-$v-upgr.*"), 0, 'Cleaning pg_upgrade log files');
    check_clean;
  }
}

# vim: filetype=perl
