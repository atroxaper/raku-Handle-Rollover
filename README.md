[![Build Status](https://travis-ci.org/atroxaper/raku-Handle-Rollover.svg?branch=master)](https://travis-ci.org/atroxaper/raku-Handle-Rollover)

# NAME

`IO::Handle::Rollover` - `IO::Handle` for rollover files (logs, for example) based on its size or creation time.

# SYNOPSIS

```perl6
use IO::Handle::Rollover;

# Open output.txt file for writing and auto rollover it
# after 30M size has been reached. Store only ten last files.
my $h = open('output.txt', :w, :rollover, :file-size<30M>, :10history-size);

$h.close; # do rollover after close;
```

# DESCRIPTION

`IO::Handle::Rollover` provides an `open` method with `:rollover` parameter:

```perl6
multi sub open(
  IO() $path,
  :$rollover! where * == True,
  Str :$file-size, # approximately maximum size of each file
  Int :$rotation-time, # time between rotations, starts from midnight + $midnight-offset
  Int:D :$history-size = 0, # maximum amount of files
  :&callback = -> $path { }, # callback with one positional arg - name of closed file
  Int:D :$midnight-offset = 0, # when midnight starts, in seconds
  :&ticker = -> { time }, # the custom way to provide current time in seconds
  Str :$suffix-style,
  :$async = False,
  |c # parameters for open each file through general open routine
)
```

Rollover bases on maximum file size or time limiter. You can use `:$file-size` and `:$rotation-time` accordingly. 

File size accepts values like 3B, 4K, 5M or 6G for corresponding amount of bytes, kilobytes, megabytes and gigabytes accordingly. The file's concrete size can be a little bit more the value because the file will be rotated only after the limit reach.

Rotation time is an integer value in seconds describe how ofter the file will be rotated. The file's concrete rotate time can be slightly after the value because the file will be rotated only after the limit reach. You can use `:$midnight-offset` argument to set the preferable 'midnight'. It is useful if you want to rollover the file each day at 3 am, then pass `:86400rotation-time, :10800midnight-offset`. You can also provide the custom `:$ticker` - routine, which returns 'current' time in seconds.

`:$history-size` describes the maximum amount of the last rollover files. Zero meens infinite. The oldest file will be deleted after the limit reach.

`:$suffix-style` may have 'time' or 'order' value. In the 'time' case, rollover files will have `_YYYY_MM_DDTHH_MM_SS` suffix. In the 'order' case - `_N` suffix, where `N` is the number (starts from 1), the smaller, the more recent the file is. In other words, in the 'order' case, each file renames from `_N` to `_{N+1}` before each rollover. The default value is 'time' for time-based rollover and 'order' for size-based rollover.

You can provide `:&callback` routine with a single argument - the new closed file. The callback will be called after each file close and rename. You can use it for your custom purpose, for example, to archive the file.

If you want to write in asynchronous mode, then pass `:async` arg.

Any other named argument will be pass to general open routing to open each new file. `:truncate` argument means there will be no any new files - the `$path` file will be recreated after time or size limit reach.

# USE FOR LOGGING

You can use `IO::Handle::Rollover` as any other output handles, but it is more useful in log systems.

For example, in the `Log::Async`:

```perl6
use Log::Async;
use IO::Handle::Rollover;

logger.send-to(open("log.txt", :w, :rollover, :60rotation-time));
info "info";
```

Or in the `LogP6`:

```perl6
use LogP6 :configure;
use IO::Handle::Rollover;

set-default-handle(open("log.txt", :w, :rollover, :60rotation-time))
get-logger("").info("info");
```

Or in the `LogP6` configuration file:

```json
{
...
  "handle": {
    "type": "custom",
    "require": "IO::Handle::Rollover",
    "fqn-method": "IO::Handle::Rollover::EXPORT::DEFAULT::&open",
    "positional": [ "log.txt" ],
    "args": {
      "w": true,
      "rollover": true,
      "file-size": "10M",
      "history-size": 3
    }
  }
...
}
```

# AUTHOR

Mikhail Khorkov <atroxaper@cpan.org>

Source can be located at: [github](https://github.com/atroxaper/raku-Handle-Rollover). Comments and Pull Requests
are welcome.

# COPYRIGHT AND LICENSE

Copyright 2020 Mikhail Khorkov

This library is free software; you can redistribute it and/or modify it under
the Artistic License 2.0.
