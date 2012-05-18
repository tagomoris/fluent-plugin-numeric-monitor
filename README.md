# fluent-plugin-numeric-monitor

## Component

### NumericMonitorOutput

Plugin to calculate min/max/avg and specified percentile values, which used in notifications (such as fluent-plugin-notifier)

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

    {"min":3012,"max":913243,"avg":100123.51,"percentile_90":154390,"percentile_95":223110}

## TODO

* more tests
* more documents

## Copyright

* Copyright
  * Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
