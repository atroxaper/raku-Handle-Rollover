use LogP6 :configure;

my \log = get-logger('main');
log.info($_) for 1..10;
