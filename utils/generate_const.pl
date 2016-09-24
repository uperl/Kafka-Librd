use strict;
use warnings;
use FindBin;
 
my @const = qw(
RD_KAFKA_PRODUCER
RD_KAFKA_CONSUMER
);
 
open my $fh, ">", "$FindBin::Bin/../const_xs.inc" or die $!;
 
for (sort @const) {
    print $fh <<EOC;
int
krd_$_()
    CODE:
        RETVAL = $_;
    OUTPUT:
        RETVAL
 
EOC
}
