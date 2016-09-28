use 5.008;
use strict;
use warnings;
use ExtUtils::MakeMaker 6.64;

WriteMakefile(
    NAME                => 'Kafka::Librd',
    AUTHOR              => q{Pavel Shaydo <zwon@cpan.org>},
    LICENSE             => 'perl',
    VERSION_FROM        => 'lib/Kafka/Librd.pm',
    ABSTRACT_FROM       => 'lib/Kafka/Librd.pm',
    OBJECT              => 'Rdkafka.o rdkafkaxs.o',
    LIBS                => '-lrdkafka',
    PL_FILES            => {},
    PREREQ_PM => {
    },
    TEST_REQUIRES => {
        'Test::More' => 0.94,
    },
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 6.64,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Kafka-Librd-*' },
    depend              => { 'Rdkafka.c' => 'const_xs.inc' },
    META_MERGE => {
        resources => {
            homepage   => 'https://github.com/trinitum/perl-Kafka-Librd',
            bugtracker => 'https://github.com/trinitum/perl-Kafka-Librd/issues',
            repository => 'git://github.com/trinitum/perl-Kafka-Librd',
            license    => 'http://dev.perl.org/licenses/',
        },
        x_contributors => [
            'Pavel Shaydo <zwon@cpan.org>',
        ],
    },
);

sub MY::postamble {
    return <<CONST;
const_xs.inc : utils/generate_const.pl
\t$^X utils/generate_const.pl

CONST
}
