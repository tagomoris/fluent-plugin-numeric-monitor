# fluent-plugin-numeric-monitor

## Component

### NumericMonitorOutput

Plugin to calculate min/max/avg and specified percentile values (and 'num' of matched messages), which used in notifications (such as fluent-plugin-notifier)

## Configuration

### NumericMonitorOutput

To calculate about HTTP requests duration (microseconds) in 'duraion', with 90 and 95 percentile values:

    <match apache.log.**>
      type numeric_monitor
      unit minute
      tag monitor.duration
      aggregate all
      input_tag_remove_prefix apache.log
      monitor_key duration
      percentiles 90,95
    </match>

Output messages like:

    {"min":3012,"max":913243,"avg":100123.51,"percentile_90":154390,"percentile_95":223110,"num":50012}


## Parameters

* monitor\_key (required)

    The key to monitor in the event record.
    
* percentiles

    Activate the percentile monitoring. Must be specified between `1` and `99` by integer separeted by , (comma). 

* tag

    The output tag. Default is `monitor`. 

* tag\_prefix

    The prefix string which will be added to the input tag. `output_per_tag yes` must be specified together. 
    
* input\_tag\_remove\_prefix

    The prefix string which will be removed from the input tag. 

* count\_interval

    The interval time to monitor in seconds. Default is `60`. 
    
* unit

    The interval time to monitor specified an unit (either of `minute`, `hour`, or `day`). 
    Use either of `count_interval` or `unit`.
    
* aggregate

    Calculate in each input `tag` separetely, or `all` records in a mass. Default is `tag`
    
* output\_per\_tag

    Emit for each input tag. `tag_prefix` must be specified together. Default is `no`. 

* output\_key\_prefix

    The prefix string which will be added to the output key. 
    
* samples\_limit

    The limit number of sampling. Default is `1000000`. 

## TODO

* more tests
* more documents

## Copyright

* Copyright
  * Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
