unit module IO::Handle::Rollover;

class X::IO::Handle::Rollover::WrongFileSize is Exception {
  method message() {
    'Str :$file-size must be like / (\d+)(<[B K M G]>) / (for example, 34B, 4K, 5M or 6G).';
  }
}

class X::IO::Handle::Rollover::WrongRotationTime is Exception {
  method message() {
    'Int :$rotation-time (in minutes) must be greater then zero.';
  }
}

class X::IO::Handle::Rollover::TimeOrSize is Exception {
  method message() {
    'One of Str :$file-size (for example, 3B, 4K, 5M or 6G) or Int :$rotation-time (in minutes) must be initialized.';
  }
}

class X::IO::Handle::Rollover::SuffixStyle is Exception {
  method message() {
    ':$suffix-style can be only any <order time>.';
  }
}

my class HandleHolder {
  has IO::Path $!path is built;
  has &!openner is built;
  has &!closer is built;

  has $!handle;
  has $!open-time;

  method open(--> IO::Handle) {
    my $handle = &!openner($!path);
    my $time = DateTime.now;
    atomic-assign($!handle, $handle);
    atomic-assign($!open-time, $time);
    $handle
  }

  method close($handle? --> Nil) {
    my $h = $handle // atomic-fetch($!handle);
    &!closer($h) with $h;
  }

  method current-handle(--> IO::Handle) {
    atomic-fetch($!handle)
  }

  method open-time() {
    atomic-fetch($!open-time);
  }

  method file-size() {
    $!path.e ?? $!path.s !! 0
  }
}

my class TimeLimiter {
  has int $!interval is built;
  has int $!midnight-offset is built;
  has &!ticker is built;

  has atomicint $!cur-limit;

  submethod TWEAK() {
    my $time = &!ticker();
    my $midnight = DateTime.new($time)
      .in-timezone(DateTime.now.timezone)
      .clone(:0hour, :0minute, :0second)
      .posix;
    my $l-cur-limit = $midnight + $!midnight-offset;
    $l-cur-limit -= $!interval while $l-cur-limit > $time;
    atomic-assign($!cur-limit, $l-cur-limit);
  }

  method cur-limit() {
    atomic-fetch($!cur-limit);
  }

  method next-limit(--> Nil) {
    my $l-cur-limit = atomic-fetch($!cur-limit);
    my $time = &!ticker();
    $l-cur-limit += $!interval while $l-cur-limit <= $time;
    atomic-assign($!cur-limit, $l-cur-limit);
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

  method rollover($open-time --> IO::Path) { ... }
}

my class TruncateRollover does Rollover {
  has IO::Path $!path is built;

  method rollover($open-time --> IO::Path) {
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
  
  method rollover($open-time --> IO::Path) {
    my @exists := $!path.parent.dir
      .grep(-> $p { $p.f && $p.basename.match($!regex) })
      .sort.List;
    @exists.tail(* - $!history-size + 1).map(*.unlink) if $!history-size > 0;
    for ($!history-size > 0 ?? @exists.head($!history-size - 1) !! @exists).reverse -> $p {
      $p.Str
        .subst($!regex, -> $m { "$!name" ~ '_' ~ $m.list[0].Str.succ ~ "$!ext" })
        .map( -> $n { $p.rename($n) });
    }
    $!path.rename($!first-file);
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

  method rollover($open-time --> IO::Path) {
    if $!history-size > 0 {
      $!path.parent.dir
        .grep(-> $p { $p.basename.match($!regex) } )
        .sort
        .head(* - $!history-size + 1)
        .map(*.unlink);
    }
    my $suffix =
      sprintf('%.4d_%.2d_%.2dT%.2d_%.2d_%.2d', .year, .month, .day, .hour, .minute, .whole-second)
      with $open-time;
    my $first-file = "$!path-name" ~ "_$suffix$!ext".IO;
    $!path.rename($first-file);
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

  has Lock $!lock = Lock.new;
  has $!closed = False;

  submethod TWEAK() {
    $!holder.open;
    $!cur-size ⚛= $!holder.file-size;
  }

  method close(IO::Handle:D: --> True) {
    $!lock.protect({
      unless $!closed {
        $!holder.close;
        my $new-file = $!rollover.rollover($!holder.open-time);
        &!callback($new-file);
        $!closed = True;
      }
    });
  }
  
  method WRITE(IO::Handle:D: Blob:D $buf --> True) {
    my int $buf-len = $buf.bytes;
    my int $n-cur-size;
    my int $l-cur-size;

    loop {
      # we heed to wait rollover actions in case of current maximus is exceeded
      repeat { $l-cur-size = ⚛$!cur-size } while $l-cur-size >= $!max-size;

      # now we 'tell' others that we will write soon
      $!writers⚛++;

      # loop for writing. we can leave that loop without write only in case
      # somebody will rollover a handler before our write
      try loop {
        $n-cur-size = $l-cur-size + $buf-len;
        # enter into write section
        if cas($!cur-size, $l-cur-size, $n-cur-size) == $l-cur-size {
          my $handle := $!holder.current-handle;
          if ($n-cur-size < $!max-size) {
            # we a just writer
            $handle.WRITE($buf);
          } else {
            my $resetted = False;
            # we a rollover writer and will do rollover actions
            my int $l-writers;
            # we need to wait all writers to finish writing or leave the loop
            repeat { $l-writers = ⚛$!writers } while $l-writers != 1;
            $handle.WRITE($buf);

            $!holder.close($handle);
            my $new-file = $!rollover.rollover($!holder.open-time);
            $!holder.open;
            # reset current sum before callback to unleash other threads asap
            $!cur-size ⚛= $!holder.file-size; $resetted = True;
            &!callback($new-file);

            # if something went wrong then reset current sum to prevent all thread stuck at least
            LEAVE { $!cur-size ⚛= 0 unless $resetted }
          }
          $!writers⚛--;
          return True;
        }
        # we could not enter to write section 
        $l-cur-size = ⚛$!cur-size;
        # in case there is no rollover actions now try one more time to enter
        last if $l-cur-size >= $!max-size;
      }
      $!writers⚛--;
    }
  }

  method READ(IO::Handle:D: Int:D $bytes --> Buf:D) { Buf.new }

  method EOF(IO::Handle:D: --> Bool:D) { True }
}

my class SizeHandleAsync is IO::Handle {
  has HandleHolder $!holder is built;
  has &!callback is built;
  has Rollover $!rollover is built;
  has $!max-size is built;

  has atomicint $!cur-size = 0;

  has Lock $!lock = Lock.new;
  has Channel $!channel = Channel.new;

  method close(IO::Handle:D: --> True) {
    $!lock.protect({
      with $!channel {
        $!channel.close;
        await $!channel.closed;
        $!holder.close;
        my $new-file = $!rollover.rollover($!holder.open-time);
        &!callback($new-file);
        $!channel = Nil;
      }
    });
  }

  submethod TWEAK() {
    $!holder.open;
    $!cur-size ⚛= $!holder.file-size;

    start {
      react {
        whenever $!channel -> $buf {
          my $handle = $!holder.current-handle;
          my $buf-len = $buf.bytes;
          my $l-cur-size = ⚛$!cur-size;
          my $n-cur-size = $l-cur-size + $buf-len;
          $handle.WRITE($buf);
          $!cur-size ⚛= $n-cur-size;
          if $n-cur-size >= $!max-size {
            $!holder.close($handle);
            my $new-file = $!rollover.rollover($!holder.open-time);
            $!holder.open;
            $!cur-size ⚛= $!holder.file-size;
            &!callback($new-file);
          }
        }
      }
    }
  }

  method WRITE(IO::Handle:D: Blob:D $buf --> True) {
    $!channel.send($buf);
  }

  method READ(IO::Handle:D: Int:D $bytes --> Buf:D) { Buf.new }

  method EOF(IO::Handle:D: --> Bool:D) { True }
}

my class TimeHandle is IO::Handle {
  has HandleHolder $!holder is built;
  has &!callback is built;
  has Rollover $!rollover is built;
  has TimeLimiter $!limiter is built;
  has &!ticker is built;

  has atomicint $!roll = 0;
  has atomicint $!writers = 0;

  has Lock $!lock = Lock.new;
  has $!closed = False;

  submethod TWEAK() {
    $!holder.open;
    $!limiter.next-limit();
  }

  method close(IO::Handle:D: --> True) {
    $!lock.protect({
      unless $!closed {
        $!holder.close;
        my $new-file = $!rollover.rollover($!holder.open-time);
        &!callback($new-file);
        $!closed = True;
      }
    });
  }

  method WRITE(IO::Handle:D: Blob:D $buf --> True) {
    loop {
      while ⚛$!roll > 0 { }
      $!writers⚛++;
      if ⚛$!roll > 0 {
        $!writers⚛--;
        next;
      }
      try {
        my $handle := $!holder.current-handle;
        if &!ticker() <= $!limiter.cur-limit() {
          $handle.WRITE($buf);
        } else {
          if cas($!roll, 0, 1) != 0 {
            $!writers⚛--;
            next;
          }
          my $resetted = False;
          while ⚛$!writers != 1 { }
          $handle.WRITE($buf);

          {
            $!holder.close($handle);
            my $new-file = $!rollover.rollover($!holder.open-time);
            $!holder.open;
            $!limiter.next-limit;
            # reset roll lock before callback to unleash other threads asap
            $!roll ⚛= 0; $resetted = True;
            &!callback($new-file);
            
            LEAVE { $!roll ⚛= 0 unless $resetted }
          }
        }
        last;
      }
    }
    $!writers⚛--;
  }

  method READ(IO::Handle:D: Int:D $bytes --> Buf:D) { Buf.new }

  method EOF(IO::Handle:D: --> Bool:D) { True }
}

my class TimeHandleAsync is IO::Handle {
  has HandleHolder $!holder is built;
  has &!callback is built;
  has Rollover $!rollover is built;
  has TimeLimiter $!limiter is built;
  has &!ticker is built;

  has Lock $!lock = Lock.new;
  has Channel $!channel = Channel.new;

  method close(IO::Handle:D: --> True) {
    $!lock.protect({
      with $!channel {
        $!channel.close;
        await $!channel.closed;
        $!holder.close;
        my $new-file = $!rollover.rollover($!holder.open-time);
        &!callback($new-file);
        $!channel = Nil;
      }
    });
  }

  submethod TWEAK() {
    $!holder.open;
    $!limiter.next-limit();

    start {
      react {
        whenever $!channel -> $buf {
          my $handle = $!holder.current-handle;
          $handle.WRITE($buf);
          if &!ticker() > $!limiter.cur-limit() {
            $!holder.close($handle);
            my $new-file = $!rollover.rollover($!holder.open-time);
            $!holder.open;
            $!limiter.next-limit;
            &!callback($new-file);
          }
        }
      }
    }
  }

  method WRITE(IO::Handle:D: Blob:D $buf --> True) {
    $!channel.send($buf);
  }

  method READ(IO::Handle:D: Int:D $bytes --> Buf:D) { Buf.new }

  method EOF(IO::Handle:D: --> Bool:D) { True }
}

my sub size-bytes(Str $size) {
  my ($num, $l) = ($0.Int, $1.Str) with $size ~~ /^ (\d+)(<[B K M G]>) $/;
  return 0 if $num < 1;
  my $result = $num;
  for <B K M> -> $L {
    last if $l eq $L;
    $result *= 1024;
  }
  return $result;
}

multi sub open(IO() $path,
  :$rollover! where * == True,
  Str :$file-size, # approximately maximum size of each file
  Int :$rotation-time, # time between rotation, starts from midnight + $midnight-offset
  Int:D :$history-size = 0, # maximus amount of files
  :&callback = -> $path { }, # callback with one positional arg - name on closed file
  Int:D :$midnight-offset = 0, # when midnight is starts
  :&ticker = -> { time }, # custom way to provide current time in seconds
  Str :$suffix-style,
  :$async = False,
  |c
) is export {
  X::IO::Handle::Rollover::SuffixStyle.new.throw if $suffix-style && not $suffix-style ~~ any <order time>;
  X::IO::Handle::Rollover::WrongFileSize.new.throw if $file-size && not $file-size ~~ / (\d+)(<[B K M G]>) /;
  X::IO::Handle::Rollover::WrongRotationTime.new.throw if $rotation-time && $rotation-time < 0;
  my $max-size = size-bytes($file-size // "0B");
  my $max-time = $rotation-time // 0;
  X::IO::Handle::Rollover::TimeOrSize.new.throw if $max-size == 0 && $max-time == 0 or $max-size > 0 && $max-time > 0;

  my %args = c.hash;
  my $history = max(0, $history-size);
  my &openner = -> $path { open($path, |%args) };
  my &closer = -> $handle {
    $handle.close;
    # sometimes close may fail because of race condition. try to stay a second
    CATCH { default { sleep 1; $handle.close }}
  };
  

  my $roller = 
    (%args<truncate> // '')
      ?? TruncateRollover.new(:$path)
      !! ($suffix-style // '') eq 'order'
        ?? OrderRollover.new(:$path, :history-size($history))
        !! ($suffix-style // '') eq 'time'
          ?? TimeRollover.new(:$path, :history-size($history))
          !! $max-size > 0
            ?? OrderRollover.new(:$path, :history-size($history))
            !! TimeRollover.new(:$path, :history-size($history));
  
  my $limiter = $max-time == 0 ?? Any !!
    TimeLimiter.new(:interval($max-time), :$midnight-offset, :&ticker);
  
  my $holder = HandleHolder.new(:$path, :&openner, :&closer);

  my $result =
    $async
      ?? $max-size > 0
        ?? SizeHandleAsync.new(:$holder, :&callback, :rollover($roller), :$max-size)
        !! TimeHandleAsync.new(:$holder, :&callback, :rollover($roller), :$limiter, :&ticker)
      !! $max-size > 0
        ?? SizeHandle.new(:$holder, :&callback, :rollover($roller), :$max-size)
        !! TimeHandle.new(:$holder, :&callback, :rollover($roller), :$limiter, :&ticker);

  $result.nl-out = $_ with %args<nl-out>;
  $result.encoding('bin') if %args<bin>;
  $result.encoding($_) with %args<enc>;
  $result.encoding($result.encoding);

  $result
}
