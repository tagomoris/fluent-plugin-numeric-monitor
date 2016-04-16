class Fluent::NumericMonitorOutput < Fluent::Output
  Fluent::Plugin.register_output('numeric_monitor', self)

  # Define `log` method for v0.10.42 or earlier
  unless method_defined?(:log)
    define_method("log") { $log }
  end

  # Define `router` method of v0.12 to support v0.10.57 or earlier
  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  EMIT_STREAM_RECORDS = 100

  config_param :count_interval, :time, default: 60,
               desc: 'The interval time to monitor in seconds.'
  config_param :unit, default: nil do |value|
    case value
    when 'minute' then 60
    when 'hour' then 3600
    when 'day' then 86400
    else
      raise Fluent::ConfigError, "unit must be one of minute/hour/day"
    end
  end
  config_param :tag, :string, default: 'monitor',
               desc: 'The output tag.'
  config_param :tag_prefix, :string, default: nil,
               desc: <<-DESC
The prefix string which will be added to the input tag.
output_per_tag yes must be specified together.
DESC

  config_param :output_per_tag, :bool, default: false,
               desc: <<-DESC
Emit for each input tag.
tag_prefix must be specified together.
DESC
  config_param :aggregate, default: 'tag',
               desc: 'Calculate in each input tag separetely, or all records in a mass.' do |val|
    case val
    when 'tag' then :tag
    when 'all' then :all
    else
      raise Fluent::ConfigError, "aggregate MUST be one of 'tag' or 'all'"
    end
  end
  config_param :input_tag_remove_prefix, :string, default: nil,
               desc: 'The prefix string which will be removed from the input tag.'
  config_param :monitor_key, :string,
               desc: 'The key to monitor in the event record.'
  config_param :output_key_prefix, :string, default: nil,
               desc: 'The prefix string which will be added to the output key.'
  config_param :percentiles, default: nil,
               desc: 'Activate the percentile monitoring. ' \
                     'Must be specified between 1 and 99 by integer separeted by , (comma).' do |val|
    values = val.split(",").map(&:to_i)
    if values.select{|i| i < 1 or i > 99 }.size > 0
      raise Fluent::ConfigError, "percentiles MUST be specified between 1 and 99 by integer"
    end
    values
  end

  config_param :samples_limit, :integer, default: 1000000,
               desc: 'The limit number of sampling.'
  config_param :interval, :float, default: 0.5

  attr_accessor :count, :last_checked

  def configure(conf)
    super

    @count_interval = @unit if @unit

    if @input_tag_remove_prefix
      @removed_prefix_string = @input_tag_remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end

    @key_prefix_string = ''
    if @output_key_prefix
      @key_prefix_string = @output_key_prefix + '_'
    end

    if (@output_per_tag || @tag_prefix) && (!@output_per_tag || !@tag_prefix)
      raise Fluent::ConfigError, 'Specify both of output_per_tag and tag_prefix'
    end
    @tag_prefix_string = @tag_prefix + '.' if @output_per_tag

    @count = count_initialized
    @mutex = Mutex.new
  end

  def start
    super
    start_watch
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
  end

  def start_watch
    # for internal, or tests
    @watcher = Thread.new(&method(:watch))
  end

  def watch
    @last_checked = Fluent::Engine.now
    while true
      sleep @interval
      if Fluent::Engine.now - @last_checked >= @count_interval
        now = Fluent::Engine.now
        flush_emit
        @last_checked = now
      end
    end
  end

  def count_initialized(keys=nil)
    # counts['tag'] = {:min => num, :max => num, :sum => num, :num => num [, :sample => [....]]}
    if @aggregate == :all
      if @percentiles
        {'all' => {min: nil, max: nil, sum: nil, num: 0, sample: []}}
      else
        {'all' => {min: nil, max: nil, sum: nil, num: 0}}
      end
    elsif keys
      values = if @percentiles
                 Array.new(keys.length) {|i| {min: nil, max: nil, sum: nil, num: 0, sample: []}}
               else
                 Array.new(keys.length) {|i| {min: nil, max: nil, sum: nil, num: 0}}
               end
      Hash[[keys, values].transpose]
    else
      {}
    end
  end

  def stripped_tag(tag)
    return tag unless @input_tag_remove_prefix
    return tag[@removed_length..-1] if tag.start_with?(@removed_prefix_string) and tag.length > @removed_length
    return tag[@removed_length..-1] if tag == @input_tag_remove_prefix
    tag
  end

  def generate_fields(count, key_prefix = '', output = {})
    output[key_prefix + 'num'] = count[:num] if count[:num]
    output[key_prefix + 'min'] = count[:min] if count[:min]
    output[key_prefix + 'max'] = count[:max] if count[:max]
    output[key_prefix + 'avg'] = (count[:sum] / (count[:num] * 1.0)) if count[:num] > 0
    output[key_prefix + 'sum'] = count[:sum] if count[:sum]
    if @percentiles
      sorted = count[:sample].sort
      @percentiles.each do |p|
        i = (count[:num] * p / 100).floor
        if i > 0
          i -= 1
        end
        output[key_prefix + "percentile_#{p}"] = sorted[i]
      end
    end
    output
  end

  def generate_output(count)
    if @aggregate == :all
      if @output_per_tag
        # tag_prefix_all: { 'key_prefix_min' => -10, 'key_prefix_max' => 10, ... } }
        output = {'all' => generate_fields(count['all'], @key_prefix_string)}
      else
        # tag: { 'key_prefix_min' => -10, 'key_prefix_max' => 10, ... }
        output = generate_fields(count['all'], @key_prefix_string)
      end
    else
      output = {}
      if @output_per_tag
        # tag_prefix_tag1: { 'key_prefix_min' => -10, 'key_prefix_max' => 10, ... }
        # tag_prefix_tag2: { 'key_prefix_min' => -10, 'key_prefix_max' => 10, ... }
        count.keys.each do |tag|
          output[stripped_tag(tag)] = generate_fields(count[tag], @key_prefix_string)
        end
      else
        # tag: { 'key_prefix_tag1_min' => -10, 'key_prefix_tag1_max' => 10, ..., 'key_prefix_tag2_min' => -10, 'key_prefix_tag2_max' => 10, ... }
        count.keys.each do |tag|
          key_prefix = @key_prefix_string + stripped_tag(tag) + '_'
          generate_fields(count[tag], key_prefix, output)
        end
      end
    end
    output
  end

  def flush
    flushed,@count = @count,count_initialized(@count.keys.dup)
    generate_output(flushed)
  end

  def flush_emit
    if @output_per_tag
      time = Fluent::Engine.now
      flush.each do |tag, message|
        router.emit(@tag_prefix_string + tag, time, message)
      end
    else
      router.emit(@tag, Fluent::Engine.now, flush)
    end
  end

  def countups(tag, min, max, sum, num, sample)
    if @aggregate == :all
      tag = 'all'
    end

    @mutex.synchronize do
      c = (@count[tag] ||= {min: nil, max: nil, sum: nil, num: 0})

      if c[:min].nil? or c[:min] > min
        c[:min] = min
      end
      if c[:max].nil? or c[:max] < max
        c[:max] = max
      end
      c[:sum] = (c[:sum] || 0) + sum
      c[:num] += num

      if @percentiles
        c[:sample] ||= []
        if c[:sample].size + sample.size > @samples_limit
          (c[:sample].size + sample.size - @samples_limit).times do
            c[:sample].delete_at(rand(c[:sample].size))
          end
        end
        c[:sample] += sample
      end
    end
  end

  def emit(tag, es, chain)
    min = nil
    max = nil
    sum = 0
    num = 0
    sample = if @percentiles then [] else nil end

    es.each do |time,record|
      value = record[@monitor_key]
      next if value.nil?

      value = value.to_f
      if min.nil? or min > value
        min = value
      end
      if max.nil? or max < value
        max = value
      end
      sum += value
      num += 1

      if @percentiles
        sample.push(value)
      end
    end
    if @percentiles && sample.size > @samples_limit
      (sample.size - @samples_limit / 2).to_i.times do
        sample.delete_at(rand(sample.size))
      end
    end
    countups(tag, min, max, sum, num, sample)

    chain.next
  end
end
