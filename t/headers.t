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

my $first_row = 
    'patid,BLOOD_PRESSURE,MRI,age,Gender,"Height cm"';

my $ra_expected_columns = 
    ['patid', 'blood_pressure', 'mri','age','gender','height_cm'];

cmp_deeply(
    $dbfy->extract_column_headers( $first_row ),
    $ra_expected_columns,
    'extract_column_headers'
);

done_testing();