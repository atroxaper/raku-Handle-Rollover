use Test;

use lib 'lib';
use IO::Handle::Rollover;

plan 10;

my $path = ($*TMPDIR ~ '/rollover/rollover.txt').IO;
$path.parent.mkdir;
LEAVE {
  *.unlink for $path.parent.dir;
  $path.parent.rmdir;
}

throws-like { open($path, :rollover, :file-size<33D>) }, X::IO::Handle::Rollover::WrongFileSize;
throws-like { open($path, :rollover, :rotation-time(-10)) }, X::IO::Handle::Rollover::WrongRotationTime;
throws-like { open($path, :rollover, :file-size<1B>, :rotation-time(1)) }, X::IO::Handle::Rollover::TimeOrSize;
throws-like { open($path, :rollover) }, X::IO::Handle::Rollover::TimeOrSize;
throws-like { open($path, :rollover, :file-size<1B>, :suffix-style<o>) }, X::IO::Handle::Rollover::SuffixStyle;

lives-ok { open($path, :rollover, :file-size<1B>, :suffix-style<order>, :w) };
lives-ok { open($path, :rollover, :file-size<1B>, :suffix-style<time>, :w) };
lives-ok { open($path, :rollover, :rotation-time(1), :suffix-style<order>, :w) };
lives-ok { open($path, :rollover, :rotation-time(1), :suffix-style<time>, :w) };

lives-ok { open($path, :rollover, :rotation-time(1), :suffix-style<time>, ticker => -> { time }, :w) };
