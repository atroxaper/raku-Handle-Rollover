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

my $h = open($path, :rollover, :w, :file-size<6B>, :suffix-style<time>, :2history-size);
for 1..10 {
  $h.put($_);
  sleep 0.4;
}
$h.close;

my @files := $path.parent.dir(test => / 'rollover_' \d**4 '_' \d**2 '_' \d**2 'T' \d**2 '_' \d**2 '_' \d**2 ".txt" /).List;
is @files.elems, 2;
for @files.sort(*.changed) Z ("7\n8\n9\n", "10\n") -> $p {
  is $p[0].slurp, $p[1];
}


