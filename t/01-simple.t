use Test;

use lib 'lib';
use IO::Handle::Rollover;

say $*TMPDIR;
my $path = ($*TMPDIR ~ '/rollover/rollover.txt').IO;
$path.parent.mkdir;
my $leave = -> { *.unlink for $path.parent.dir }
END { $path.parent.rmdir }
my $format = '%.5d';

subtest {
  LEAVE { $leave() }
  my ($max-value, $threads, $line-size) = (10000, 10, 6);
  my $h = open($path, :rollover, :w, :file-size<1G>, :2history-size);
  await (^$threads).map: { start { $h.put(sprintf($format, $_)) for 1..$max-value }}
  $h.close;
  say 'time', now - ENTER now;

  my $file = $path.parent.dir.first;
  is $path.parent.dir.elems, 1;
  is $file.basename, 'rollover_1.txt';
  is $file.s, $max-value * $threads * $line-size;
  my %bh := BagHash.new;
  %bh.add($_) for $file.lines;
  is %bh.elems, $max-value;
  is %bh.values.grep(* != $threads).elems, 0;
}

subtest {
  LEAVE { $leave() }
  my ($max-value, $threads, $line-size) = (10000, 10, 6);
  my $h = open($path, :rollover, :w, :file-size<1G>, :2history-size, :async);
  await (^$threads).map: { start { $h.put(sprintf($format, $_)) for 1..$max-value }}
  $h.close;
  say 'time', now - ENTER now;

  my $file = $path.parent.dir.first;
  is $path.parent.dir.elems, 1;
  is $file.basename, 'rollover_1.txt';
  is $file.s, $max-value * $threads * $line-size;
  my %bh := BagHash.new;
  %bh.add($_) for $file.lines;
  is %bh.elems, $max-value;
  is %bh.values.grep(* != $threads).elems, 0;
}

done-testing;