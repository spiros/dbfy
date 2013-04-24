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

################################################
#################################### parse_row()

my $raw_row     = "1,2,3,4,5,patid";

cmp_deeply(
    $dbfy->parse_row( $raw_row ),
    [ 1,2,3,4,5,'patid' ],
    'parse_row()'
);

done_testing();
 