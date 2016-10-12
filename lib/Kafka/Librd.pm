package Kafka::Librd;
use strict;
use warnings;
our $VERSION = "0.01";
my $XS_VERSION = $VERSION;
$VERSION = eval $VERSION;

require XSLoader;
XSLoader::load('Kafka::Librd', $XS_VERSION);

use Exporter::Lite;
our @EXPORT_OK;

=head1 NAME

Kafka::Librd - bindings for librdkafka

=head1 VERSION

This document describes Kafka::Librd version 0.01

=head1 SYNOPSIS

    use Kafka::Librd;

=head1 DESCRIPTION

=head1 METHODS

=cut

sub new {
    my ( $class, $type, $params ) = @_;
    return _new( $type, $params );
}

{
    my $errors = Kafka::Librd::Error::rd_kafka_get_err_descs();
    no strict 'refs';
    for ( keys %$errors ) {
        *{__PACKAGE__ . "::RD_KAFKA_RESP_ERR_$_"} = eval "sub { $errors->{$_} }";
        push @EXPORT_OK, "RD_KAFKA_RESP_ERR_$_";
    }
}

1;

__END__

=head1 BUGS

Please report any bugs or feature requests via GitHub bug tracker at
L<http://github.com/trinitum/perl-Kafka-Librd/issues>.

=head1 AUTHOR

Pavel Shaydo C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Pavel Shaydo

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
