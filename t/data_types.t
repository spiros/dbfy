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

my $rah_tests = [

    {
        'input'  => [ 1..6 ],
        'output' => 'numeric'
    },

    {
        'input'  => [ 1..6, ],
        'output' => 'numeric'
    },

    {
        'input'  => [ 1..4, 'string' ],
        'output' => 'string'
    },
    
    {
        'input'  => [ 'string', 1,2,3, 'string' ],
        'output' => 'string'
    },

];

my $n = 0;
foreach my $rh_test ( @$rah_tests ){
    my $ra_input = $rh_test->{'input'};
    my $expected_output = $rh_test->{'output'};

    is(
        $dbfy->guess_data_type( $ra_input ),
        $expected_output,
        "guess_data_type - $n"
    );
    $n++;
}

my $rh_columns = {
    'c1' => { 
        1 => 1,
        2 => 1,
        3 => 1,
        4 => 1,
        5 => 1,
    },

    'c2' => {
        'str1'     => 1,
        1          => 1,
        'echo 123' => 1
    },

    'c3' => {
        'string' => 1,
        '1' => 1,
    },
};

my $rh_column_data_types_expected = {
    'c1' => 'numeric',
    'c2' => 'string',
    'c3' => 'string',
};

cmp_deeply(
    $dbfy->guess_column_data_types( $rh_columns ),
    $rh_column_data_types_expected,
    'guess_column_data_types'
);

done_testing();
