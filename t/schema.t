use strict;
use warnings;

use Test::More;
use Data::Dumper;
use File::Temp 'tempfile';
use Test::Deep;

use lib '../';

use_ok('dbfy');

my ($fh, $filename) = tempfile();

my $rh_params = { 
    'output' => $filename,
    'input'  => [ 't1.csv', 't2.csv', 't3.csv' ],
};

my $dbfy = dbfy->new( $rh_params );
isa_ok( $dbfy, 'dbfy' );

my $table_name = 'moo';
my $rh_column_defs = {
    'c1' => 'string',
    'c2' => 'numeric',
    'c3' => 'numeric',
    'c4' => 'string'
};

my $SQL_expected = 
    'CREATE TABLE `moo` (  `c1` TEXT , `c2` NUMERIC , `c3` NUMERIC , `c4` TEXT  )';

is(
    $dbfy->create_table_schema( $table_name, $rh_column_defs ),
    $SQL_expected,
    'create_table_schema'
);

done_testing();