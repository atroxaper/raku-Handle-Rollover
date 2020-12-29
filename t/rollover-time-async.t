use Test;

use lib 'lib';
use IO::Handle::Rollover;

plan 4;

my $*DEBUG = %*ENV<DEBUG> ?? True !! False;

my $path = ($*TMPDIR ~ '/rollover/rollover.txt').IO;
$path.parent.mkdir;
END {
  *.unlink for $path.parent.dir; 
  $path.parent.rmdir;
}

my ($max-value, $threads) = (1000, 10);
my $total-size = (1..$max-value).map(*.Str.chars + 1).sum * $threads;
my $rotation-time = 2;
my atomicint $files = 0;
my &callback = -> $ { $files⚛++ }

my $h = open($path, :w, :rollover, :$rotation-time, :async, :&callback);

my $time = now;
await (^$threads).map: { start { $h.put($_) for 1..$max-value }}
$h.close;
say "time: ", now - $time if $*DEBUG;

my %bh := BagHash.new;
my $size = 0;
is $path.parent.dir.elems, ⚛$files;
for $path.parent.dir -> $p {
  $size += $p.s;
  %bh.add($_) for $p.lines;
  say $p, ' ', $p.s if $*DEBUG;
}
is $size, $total-size;
is %bh.elems, $max-value;
is %bh.values.grep(* != $threads).elems, 0;