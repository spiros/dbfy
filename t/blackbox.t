use strict;
use warnings;

use Test::More;
use Data::Dumper;
use Test::Deep;
use DBD::SQLite;

use lib '../';

use_ok('dbfy');

my $rh_params = { 
    'output' => 'moo.sqlite',
    'input'  => [ 'tkramer.csv', 'tjerry.csv', 'tcostanza.csv' ],
};

unlink('moo.sqlite');

my $dbfy = dbfy->new( $rh_params );
isa_ok( $dbfy, 'dbfy' );

ok( $dbfy->start(), 'start()' );

my $SQLite = DBI->connect("dbi:SQLite:dbname=moo.sqlite");

##
## Count total number of rows

foreach my $table (qw( tkramer tjerry tcostanza )){

    my $sql_rows = 
        sprintf('SELECT COUNT(*) FROM %s', $table);

    my $sth = $SQLite->prepare( $sql_rows );
    $sth->execute();

    is(
        $sth->fetchrow_array,
        13,
        'number of rows in table OK'
    );

    my $rh_distinct = {
        'patid'    => 13,
        'age'      => 6,
        'gender'   => 3,
        'income'   => 6,
        'postcode' => 10,
    };

    foreach my $c ( keys %$rh_distinct ){
        my $sql_distinct = 
            sprintf('SELECT COUNT(DISTINCT(%s)) FROM %s', $c, $table);

        my $sth = $SQLite->prepare( $sql_distinct );
        $sth->execute();
        is(
            $sth->fetchrow_array(),
            $rh_distinct->{$c},
            "count distinct values for column $c"
        );
    }

    my $rh_data = {
        'patid'    => [qw( 1 10 11 12 13 2 3 4 5 6 7 8 9 )],
        'age'      => [qw( 12 19 20 33 45 88 )],
        'gender'   => [qw( 1 2 3 )],
        'income'   => [qw( 12.001 23 33 44 9 12 )], 
        'postcode' => [qw( E1 E8 E9 M1 M12 QQ1 RX12 W1 W12 W2 )],
    };

    foreach my $c ( keys %$rh_data ){
        my $sql_values = 
            sprintf('SELECT DISTINCT(%s) FROM %s', $c, $table );

        my $sth = $SQLite->prepare( $sql_values );
        $sth->execute();

        my $ra_got = [ ];
        while ( my $ra_row = $sth->fetchrow_arrayref ){
            push( @$ra_got, $ra_row->[0] );
        }

        my $ra_expected = $rh_data->{$c};

        cmp_bag(
            $ra_expected,
            $ra_got,
            "data matches for column $c"
        );

    }

}



unlink('moo.sqlite');

done_testing();