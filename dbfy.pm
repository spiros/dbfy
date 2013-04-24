package dbfy;

use strict;
use warnings;

use Data::Dumper;
use DBD::SQLite;
use Text::CSV;
use Tie::File;
use Scalar::Util 'looks_like_number';
use SQL::Abstract;
use File::Basename;

=head2 new( $rh_params )

Create a new instance of the object.

Parameters required: 
    'input'  : reference to an array of files to parse
    'output' : SQLite output file

This method will return undef on error.

=cut

sub new {
    my $class     = shift;
    my $rh_params = shift;
    my $self      = { };
    bless $self, $class;

    $self->{'config'}->{'input'}  = $rh_params->{'input'};
    $self->{'config'}->{'output'} = $rh_params->{'output'};

    $self->{'csv'}  = Text::CSV->new();
    $self->{'sqla'} = SQL::Abstract->new();

    my $dsn = $self->_create_dsn( $self->{'config'}{'output'} );

    $self->{'dbh'} = DBI->connect( $dsn, '', '' )
        || die "Unable to initialize DBI.";

    return $self;
}

=head2 _create_dsn( $filename )

Internal function to create the DSN used to initiate DBI.

=cut

sub _create_dsn {
    my $self   = shift;
    my $dbfile = shift;

    return sprintf("dbi:SQLite:dbname=%s", $dbfile);

}

=head2 dbh()

Internal function to return the database handle.

=cut

sub dbh {
    my $self = shift;
    return $self->{'dbh'};
}

=head2 execute( $sql [, $ra_bind ] )

Execute the specified SQL string. Optionally takes a reference to
an array of bind values to be passed along to the database
handle at the time of execution.

=cut

sub execute {
    my $self    = shift;
    my $sql     = shift;
    my $ra_bind = shift;

    my $sth = $self->dbh->prepare( $sql );

    # Execute with supplied bind values
    if ( $ra_bind ){
        return $sth->execute( @$ra_bind );
    } 

    # Execute with no bind values
    else {
        return $sth->execute( );
    }
}

=head2 start

Loop through input files and process them.

=cut

sub start {
    my $self = shift;
    my $ra_files = $self->{'config'}{'input'};

    foreach my $file (@$ra_files){
        $self->process_file( $file );
    }

    return 1;
}

=head2 process_file( $filename ) 

Process an input file. This involves:

1. Opening the file
2. Mapping column names to data types
3. Creating a table to hold the file contents
4. Loading the data in the table

This method will return undef on error.

=cut

sub process_file {
    my $self = shift;
    my $file = shift;

    open my $fh, $file 
        || die "Unable to read file: $!";

    ## open files

    my @contents;
    tie @contents, 'Tie::File', $file;

    my $total_rows = scalar(@contents);

    ## Extract column headers
    my $ra_columns = $self->extract_column_headers( $contents[0] );

    my $total_columns = scalar(@$ra_columns);
    
    my @a_columns = @$ra_columns;

    printf STDERR ("Processing file %s, %s lines, %s columns (%s)\n",
            $file,
            $total_rows,
            $total_columns,
            join ',', @a_columns );

    ##
    ## Read the top 10% of rows and store the values

    my $rh_top_ten_pct = { };

    # Init as empty as some columns might
    # be 100% missing. 
    
    foreach my $c (@a_columns){
        $rh_top_ten_pct->{ $c } = { };
    }

    my $tenpct = int( $total_rows * 0.1 ) || 1;

    foreach my $n ( 1..$tenpct ) {
        my $ra_row = $self->get_row_n( \@contents, $n );
        if ( $ra_row ){
            foreach my $n (0..$total_columns-1){
                my $label = $a_columns[$n];
                my $value = $ra_row->[$n];
                
                # when guessing column data types, only
                # include defined values and skip undefined
                # and empty string values

                next if ( ! defined $value );
                next if ( $value eq '' );

                $rh_top_ten_pct->{ $label }->{ $value }++;
            }
        } 
        else {
            warn "Unable to parse row: $n";
        }    
    }

    ##
    ## Guess data types for each column

    my $rh_column_data_types = $self->guess_column_data_types( $rh_top_ten_pct );

    ##
    ## Create the table

    my $table_name = $self->extract_table_name_from_file_name( $file );
    unless ( $self->create_table( $table_name, $rh_column_data_types ) ){
        die "Failed to create table: $table_name\n";
    }

    ##
    ## Load data in table

    my $c=0;
    foreach my $n ( 1..$total_rows ) {

        my $ra_row = $self->get_row_n( \@contents, $n );
        if ( $ra_row ){

            my $rh_row = $self->map_row_to_columns( $ra_row, \@a_columns );
            my ($sql, @bind) = 
                $self->{sqla}->insert( $table_name, $rh_row );

            if ( $self->execute( $sql, \@bind) ) {
                $c++;    
            } else {
                # warn of insert error
            }            
            
        } else {
            # warn of parse error
        }
    }

    printf STDERR "Loaded %s rows in table %s.\n", $c, $table_name;

    return 1;

}

=head2 map_row_to_columns

=cut

sub map_row_to_columns {
    my $self       = shift;
    my $ra_row     = shift;
    my $ra_columns = shift;

    return undef unless ( defined $ra_row );
    return undef unless ( defined $ra_columns );

    my $total_columns = scalar(@$ra_row);

    my $rh_row = { };
    foreach my $n (0..$total_columns-1){
        my $label = $ra_columns->[ $n ];
        my $value = $ra_row->[ $n ];
                
        # normalize undefined values and empty
        # string values

        if ( ! defined $value || $value eq '' ){
            $value = undef;
        }

        $rh_row->{$label} = $value;
    }

    return $rh_row;

}

=head2 get_row_n 

Given a tied file, returns a reference to an array with 
the nth row's values.

This method will return undef on error.

=cut

sub get_row_n {
    my $self = shift;
    my $file = shift;
    my $n    = shift;

    return undef unless ( defined $file );
    return undef unless ( defined $n && $n >= 0 );

    my $raw_row = $file->[ $n ];

    return undef unless ( defined $raw_row );

    $raw_row =~ s/\r//g;

    return $self->parse_row( $raw_row );

}

=head2 parse_row

Given a row in CSV format, returns a reference to an array with the
values of the row.

This method returns undef on error.

=cut

sub parse_row {
    my $self = shift;
    my $row = shift;

    return undef unless ( defined $row );

    if ( $self->{'csv'}->parse( $row ) ){
        my @fields = $self->{'csv'}->fields();
        return \@fields;
    }  

    else {
        my $error_diag = $self->{'csv'}->error_diag();
        warn "Unable to parse $row: $error_diag";
        return undef;
    }

}

=head2 guess_column_data_types( $rh_top_ten_pct )

Given a reference to a hash of columns and their associated
values returns a reference to a hash with the column data types.

    my $rh_input = { 
        'c1' => {
            'value1' => 1,
            'value2' => 1,
        },
        
        'c2' => {
            'value4'  => 1,
            'value11' => 1,
        }
    };

    my $rh_data_types = $dbfy->guess_column_data_types( $rh_input );

This method will return undef on error.

=cut

sub guess_column_data_types {
    my $self           = shift;
    my $rh_top_ten_pct = shift;

    return undef unless ( defined $rh_top_ten_pct );

    my $rh_column_data_types = { };
    foreach my $col ( keys %$rh_top_ten_pct ){
        my @values = ( keys %{ $rh_top_ten_pct->{ $col } } );

        my $data_type = $self->guess_data_type( \@values );
        $rh_column_data_types->{$col} = $data_type;
    }

    return $rh_column_data_types;
}

=head2 guess_data_type( $ra_values )

Given a reference to an array containing scalars with values
returns the best guess data type. Data types returned
include 'string' and  'numeric'.

This method will return undef on error.

=cut

sub guess_data_type {
    my $self      = shift;
    my $ra_values = shift;

    return undef unless ( defined $ra_values );
    return undef unless ( $ra_values );

    my $is_num = 1;
   
    foreach my $value ( @$ra_values ){
        if ( ! looks_like_number( $value ) ){
            $is_num = 0;
            # no point going on, this is a string
            last;
        }
    }

    # Extremely greedy typing - if it looks like
    # a number then its numeric while if it doesnt
    # then its a string. This means dates, if they
    # are not in epoch format, will always end up
    # being considered strings.

    $is_num 
        ? return 'numeric'
        : return 'string';
}

=head2 create_table_schema( $able_name, $rh_column_definitions )

Given a table name and the column data type definitions, returns
the SQL used to create the table.

=cut

sub create_table_schema {
    my $self           = shift;
    my $table_name     = shift;
    my $rh_definitions = shift;

    return undef unless ( defined $table_name );
    return undef unless ( defined $rh_definitions );
        
    my @a_coldefs;

    foreach my $column ( sort keys %$rh_definitions ){
        my $data_type = $rh_definitions->{ $column };
        my $col_def;
        if ( $data_type eq 'string' ){
            $col_def = sprintf(' `%s` TEXT ', $column );
        } else {
            $col_def = sprintf(' `%s` NUMERIC ', $column ); 
        }
        push(@a_coldefs, $col_def);
    }

    my $SQL = 
        sprintf('CREATE TABLE `%s` ( %s )', $table_name, join ',', @a_coldefs );

    return $SQL;
}

=head2 create_table( $tablename, $rh_column_data_types )

Given the table name and column data type definitions,
creates the table in the database.

This method will return undef on error;

=cut

sub create_table {
    my $self = shift;
    my $file = shift;
    my $rh_definitions = shift;

    return undef unless defined $file;
    return undef unless defined $rh_definitions;

    my $sql = $self->create_table_schema( $file, $rh_definitions );

    return $self->execute( $sql );
}

=head2 extract_table_name_from_file_name( $filename )

Extracts and normalizes the table name from the
supplied input file name. All tables names are lower
cased and spaces are replaced with underscores.

This method will return undef on error.

=cut

sub extract_table_name_from_file_name{
    my $self     = shift;
    my $path     = shift;

    return undef unless ( defined $path );

    my($filename, $directories, $suffix) = 
        fileparse($path, qr/\.[^.]*/);
   
    return $self->normalize_string( $filename );
}

=head2 extract_column_headers( $first_row )

Given a raw file row, returns a reference to 
an array with the column header names.

This method will return undef on error.

=cut

 sub extract_column_headers {
    my $self      = shift;
    my $first_row = shift;

    return undef unless ( defined $first_row );

    my $ra_columns = [ ];

    # Get rid of windows CR if its there.
    $first_row =~ s/\r//g;

    if ( $self->{csv}->parse( $first_row ) ) {
        my @raw_columns = $self->{'csv'}->fields();

        foreach my $c ( @raw_columns ) {
            $c = $self->normalize_string( $c );
            push(@$ra_columns, $c);
        }
    } 
    else {
        # This is a fatal error.
        die "Unable to map column headers.\n";
    }
    return $ra_columns;
}

=head2 normalize_string( $string )

Normalizes a string by: replacing whitespace with
underscores and lowecasing it.

This method returns undef on error.

=cut

sub normalize_string {
    my $self = shift;
    my $string = shift;

    return undef unless ( defined $string );
    return undef unless ( $string ne '' );

    $string =~ s/ /_/g;
    return lc( $string );

}


1;