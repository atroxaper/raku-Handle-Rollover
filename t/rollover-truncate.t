use Test;

use lib 'lib';
use IO::Handle::Rollover;

plan 2;

my $path = ($*TMPDIR ~ '/rollover/rollover.txt').IO;
$path.parent.mkdir;
END {
  *.unlink for $path.parent.dir; 
  $path.parent.rmdir;
}

my $h = open($path, :rollover, :w, :file-size<6B>, :truncate);
$h.put($_) for 1..10;
$h.close;

is $path.slurp, "10\n";
is $path.parent.dir.elems, 1;