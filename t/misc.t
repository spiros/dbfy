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
############## extract_table_name_from_file_name

my $rah_tests = [

    {
        'input'  => 'patients.csv',
        'output' => 'patients'
    },

    {
        'input'  => 'MASTER_pt file.csv',
        'output' => 'master_pt_file'
    },

    {
        'input'  => 'BLOOD_PRESSURE.tsv',
        'output' => 'blood_pressure'
    },

];

my $n = 0;
foreach my $rh_test ( @$rah_tests ){
    my $input = $rh_test->{'input'};
    my $expected_output = $rh_test->{'output'};

    is(
        $dbfy->extract_table_name_from_file_name( $input ),
        $expected_output,
        "guess_data_type - $n"
    );
    $n++;
}

################################################
############################### normalize_string

my $rah_tests_str = [

    {
        'input'  => 'PATIENTS',
        'output' => 'patients'
    },

    {
        'input'  => 'BLOOD PRESSURE',
        'output' => 'blood_pressure'
    },

    {
        'input'  => 'Height in_CM',
        'output' => 'height_in_cm'
    }

];

$n = 0;
foreach my $rh_test ( @$rah_tests_str ){
    my $input = $rh_test->{'input'};
    my $expected_output = $rh_test->{'output'};

    is(
        $dbfy->normalize_string( $input ),
        $expected_output,
        "normalize_string - $n"
    );
    $n++;
}

################################################
############################# map_row_to_columns

my $ra_row     = [ 1, 2, 3, 'F', 'test' ];
my $ra_columns = [ 'patid', 'arc1', 'rand2', 'gender', 'name' ];

my $rh_expected = { 
    'patid'  => 1,
    'arc1'   => 2,
    'rand2'  => 3,
    'gender' => 'F',
    'name'   => 'test'
};

cmp_deeply(
    $rh_expected,
    $dbfy->map_row_to_columns( $ra_row, $ra_columns ),
    'map_row_to_columns'
);

done_testing();