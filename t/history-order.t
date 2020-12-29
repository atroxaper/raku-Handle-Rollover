use Test;

use lib 'lib';
use IO::Handle::Rollover;

plan 3;

my $path = ($*TMPDIR ~ '/rollover/rollover.txt').IO;
$path.parent.mkdir;
END {
  *.unlink for $path.parent.dir; 
  $path.parent.rmdir;
}

my $h = open($path, :rollover, :w, :file-size<6B>, :suffix-style<order>, :2history-size);
$h.put($_) for 1..10;
$h.close;

is $path.parent.dir(test => / 'rollover_' \d '.txt' /).elems, 2;
is $path.extension('1.txt', :joiner<_>).slurp, "10\n";
is $path.extension('2.txt', :joiner<_>).slurp, "7\n8\n9\n"

