package #hide
Alien::Base::Wrapper;
sub mm_args {return(LIBS => '-lrdkafka')};

1;

__END__

=head1 README

In order to get this package to install without Alien packages I had to fake them.

To Build RPMs for CentOS 7.
  
  $ yum install epel-release
  $ yum install librdkafka-devel
  $ perl -Mlib=package Makefile.PL
  $ make dist
  $ rpmbuild -ta Kafka-Librd-*.tar.gz

For apt-get based operating systems, the development package is librdkafka-dev

=cut
