my $D = True;

unit module IO::Handle::Rollover;

my class HandleHolder {
  has IO::Path $!path is built;
  has &!openner is built;
  has &!closer is built;
  has &!ticker is built;
  has Lock $!lazy-lock = Lock.new;

  has $!handle;
  has $!open-time;

  method open() {
    my $handle = &!openner();
    atomic-assign($!handle, $handle);
    atomic-assign($!open-time, &!ticker());
    $handle;
  }

  method close($handle?) {
    my $h = $handle // self.current-handle();
    &!closer($h);
    Nil
  }

  method current-handle() {
    my $handle = atomic-fetch($!handle);
    return $_ with $handle;
    $!lazy-lock.protect({
      my $handle = atomic-fetch($!handle);
      self.open() whithout $handle;
    })
    atomic-fetch($!handle)
  }

  method open-time() {
    atomic-fetch($!open-time);
  }
}

my role Rollover {

  method prepare-path(:$path) {
    my $path-name = $path.extension('', :parts(^10));
    %(
      path-name => $path-name.Str,
      name => $path-name.basename,
      ext => ('_', $path.extension(:parts(^10)).grep(*.chars)).join(".").substr(1)
    )
  }

  method rollover(:$open-time --> IO::Path) { ... }
}

my class TruncateRollover does Rollover {
  has IO::Path $!path is built;

  method rollover(:$open-time --> IO::Path) {
    $!path
  }
}

my class OrderRollover does Rollover {
  has IO::Path $!path is built;
  has Int $!history-size is built;

  has Str $!path-name;
  has Str $!name;
  has Str $!ext;

  has $!regex;
  has Str $!first-file;

  submethod TWEAK() {
    ($!name, $!path-name, $!ext) = self.prepare-path(:$!path)<name path-name ext>;
    my ($name, $ext) = ($!name, $!ext);
    $!regex = rx/ "$name" _ (\d+) "$ext" /;
    my $format = $!history-size < 1 ?? '%d' !! '%.' ~ $!history-size.Str.chars ~ 'd';
    $!first-file = $!path-name ~ '_' ~ sprintf($format, 1) ~ $ext;
  }
  
  method rollover(:$open-time --> IO::Path) {
    my @exists := $!path.parent.dir
      .grep(-> $p { $p.f && $p.basename.match($!regex) })
      .sort.List;
    @exists.tail(* - $!history-size + 1).map(-> $p { if $D {say "unlink $p"} else {$p.unlink} }) if $!history-size > 0;
    for ($!history-size > 0 ?? @exists.head($!history-size - 1) !! @exists).reverse -> $p {
      $p.Str
        .subst($!regex, -> $m { "$!name" ~ '_' ~ $m.list[0].Str.succ ~ "$!ext" })
        .map(-> $n { if $D {say "rename $p -> $n"} else {$p.rename($n)} });
    }
    if $D {
      say "rename $!path -> $!first-file";
    } else {
      $!path.rename($!first-file);
    }
    $!first-file.IO;
  }
}

my class TimeRollover does Rollover {
  has IO::Path $!path is built;
  has Int $!history-size is built;

  has Str $!path-name;
  has Str $!name;
  has Str $!ext;

  has $!regex;

  submethod TWEAK() {
    ($!name, $!path-name, $!ext) = self.prepare-path(:$!path)<name path-name ext>;
    my ($name, $ext) = ($!name, $!ext);
    $!regex = rx/ "$name" _ \d**4 '_' \d**2 '_' \d**2 'T' \d**2 '_' \d**2 '_' \d**2 "$ext" /;
  }

  method rollover(:$open-time --> IO::Path) {
    if $!history-size > 0 {
      $!path.parent.dir
        .grep(-> $p { $p.basename.match($!regex) } )
        .sort
        .head(* - $!history-size + 1)
        .map(-> $p { if $D {say "unlink $p"} else {$p.unlink}} );
    }
    my $suffix =
      sprintf('%.4d_%.2d_%.2dT%.2d_%.2d_%.2d', .year, .month, .day, .hour, .minute, .whole-second)
      with $open-time;
    my $first-file = "$!path-name" ~ "_$suffix$!ext".IO;
    if $D {
      say "rename $!path -> $first-file";
    } else {
      $!path.rename($first-file);
    }
    $first-file.IO
  }
}

my class SizeHandle is IO::Handle {
  has HandleHolder $!holder is built;
  has &!callback is built;
  has Rollover $!rollover is built;
  has $!max-size is built;

  has atomicint $!cur-size = 0;
  has atomicint $!writers = 0;

  submethod TWEAK() {
    $!holder.current-handle;
  }
  
  method WRITE(IO::Handle:D: Blob:D $buf --> True) {
    my $buf-len = $buf.bytes;
    my $sum, $cs;

    loop {
      # we heed to wait rollover actions in case of current maximus is exceeded
      repeat { $cs = ⚛$!cur-sum } while $cs >= $!max-sum;

      # now we 'tell' others that we will write soon
      $!writers⚛++;

      # loop for writing. we can leave that loop without write only in case
      # somebody will rollover a handler before our write
      loop {
        LEAVE { $!writers⚛-- }
        $sum = $cs + $buf-len;
        # enter to write section
        if cas($!cur-sum, $cs, $sum) == $cs {
          my $ch = $!holder.current-handle;
          if ($sum < $!max-sum) {
            # we a just writer
            $ch.WRITE($buf);
          } else {
            my $resetted = False;
            # we a rollover writer and will do rollover actions
            my $cw;
            # we need to wait all writers to finish writing or leave the loop
            repeat { $cw = ⚛$!writers } while $cw != 1;
            $ch.WRITE($buf);

            $!holder.close($ch);
            my $new-file = $!rollover.rollover($!holder.open-time);
            $!holder.open;
            # reset current sum before callback to unleash other threads asap
            $!cur-sum ⚛= 0; $resetted = True;
            &!callback($new-file);

            # something went wrong - reset current sum to prevent all thread stuck at least
            LEAVE { $!cur-sum ⚛= 0 unless $resetted }
          }
          return True;
        }
        # we could not enter to write section 
        $cs = ⚛$!cur-sum;
        # in case there is no rollover actions now try one more time to enter
        last if $cs >= $!max-sum;
      }
    }
  }

  methon READ(IO::Handle:D: Int:D $bytes --> Buf:D) { Buf.new }

  method EOF(IO::Handle:D: --> Bool:D) { True }
}

my sub size-bytes(Str $size) {
  my ($num, $l) = ($0.Int, $1.Str) with $size ~~ / (\d+)(<[B K M G]>) /;
  return 0 if $num < 1;
  my $result = $num;
  for <B K M> -> $L {
    last if $l eq $L;
    $result *= 1024;
  }
  return $result;
}

multi sub open(IO() $path,
  True :$rollover!,
  Str:D :$file-size, # approximately maximum size of each file
  Int:D :$rotation-time, # time between rotation, starts from midnight + $midnight-offset
  Int:D :$history-size = 0, # maximus amount of files
  :&callback = -> $path { }, # callback with one positional arg - name on closed file
  Int:D :$midnight-offset = 0, # when midnight is starts
  :&ticker = -> { DateTime.now }, # custom way to provide current time
  Str:D :$suffix-style where { $_ ~~ any <time order> },
  Bool:D :$async = False,
  Bool:D :$lazy = False,
  |c
) {
  my %args = c.hash;
  my &openner = -> $path { open($path, |%args) };
  my &closer = -> $handle { $handle.close };
  my $max-size = size-bytes($file-size);
  
  my $holder = HandleHolder.new(:$path, :&openner, :&closer, :&ticker);
  $holder.current-handle unless $lasy;
  my $roller = 
    %args<truncate>
      ?? TruncateRollover.new(:$path)
      !! $suffix-style eq 'order'
        ?? OrderRollover.new(:$path, :$history-size)
        !! TimeRollover.new(:$path, :$history-size);

  my $result = SizeHandle.new(:$holder, :&callback, :rollover($roller), :$max-size);

  $result.chomp = $_ with %args<chomp>;
  $result.nl-out = $_ with %args<nl-out>;
  $resutl.out-buffer = $_ with %args<out-buffer>;
  $reulst.encoding('bin') if %args<bin>;
  $result.encoding($_) with %args<enc>;
}

OrderRollover.new(
  :history-size(0),
  :path('/Users/atroxaper/idea/raku/modules/todel/foo.txt'.IO)).rollover(:open-time(4));


#`[class IO::Handle::Rollover is IO::Handle {
  has IO::Path $!basepath;
  has Int $!history-size;
  has &!to-write;
  has &!to-roll;
  has &!open-file;
  has &!close-callback;
  has &!ticker;

  has Int $!dead-size;
  has DateTime $!base-date;
  has $!next-date-diff;

  has IO::Handle $!handle;
  has $!last-open-time;
  has atomicint $!write-lock = 0;
  has atomicint $!roll-lock = 0;
  has atomicint $!dead-time = 0;
  has atomicint $!current-size = 0;

  has @!handle-changes;

  method new(
    IO() $basepath,
    Int :$last-files where { $_  > 0 } = 5,
    Str :$suffix-style where { $_ ~~ any <time order> } = 'order',
    Str :$max-file-size where { $_ ~~ / (\d+)(<[B K M G]>) / } = '0B',
    Str :$roll-every where { $_ ~~ any <none minute hour day week month year> } = 'none',
    :&close-callback where .signature ~~ :($) = -> $ { },
    :&ticker where { .signature ~~ :( --> Int) } = -> { time },
  ) {
    my $dead-size = size-bytes($max-file-size);
    my $base-date = base-date($roll-every);
    my $next-date-diff = $roll-every => 1;
    my &to-roll = $suffix-style eq 'order'
      ?? -> { self!roll-order }
      !! -> { self!roll-time }
    my &to-write;
    if $base-date.defined && $dead-size > 0 {
      &to-write = -> $buf { self!write-time-and-size($buf) }
    } elsif $base-date.defined {
      &to-write = -> $buf { self!write-time($buf) }
    } elsif $dead-size > 0 {
      &to-write = -> $buf { self!write-size($buf) }
    } else {
      die 'fooo';
    }

  }


  method open(*%open-params) {
    &!open-file = -> { my $h = IO::Handle.new(:path($!basepath)); $h.open(|%open-params); $h }
    $!basepath.mkdir;
    atomic-assign($!handle, &!open-file($!basepath));
    atomic-assign($!last-open-time, DateTime.now);
  }

  sub size-bytes(Str $size) {
    my ($num, $l) = ($0.Int, $1.Str) with $size ~~ / (\d+)(<[B K M G]>) /;
    return 0 if $num < 1;
    my $result = $num;
    for <B K M> -> $L {
      last if $l eq $L; $result *= 1024;
    }
    return $result;
  }

  sub base-date(Str $time) {
    my $n = DateTime.now;
    given $time {
      when 'none' { return DateTime }
      when 'week' { return $n.ealier(day => $n.day-of-week - 1).truncated-to('day') }
      default { return $n.truncated-to($time) }
    }
  }
}

]