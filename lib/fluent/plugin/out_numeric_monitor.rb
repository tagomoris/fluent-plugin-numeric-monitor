class Fluent::NumericMonitorOutput < Fluent::Output
  Fluent::Plugin.register_output('numeric_monitor', self)

  EMIT_STREAM_RECORDS = 100

  config_param :count_interval, :time, :default => 60
  config_param :unit, :string, :default => nil
  config_param :tag, :string, :default => 'monitor'
  config_param :tag_prefix, :string, :default => nil

  config_param :output_per_tag, :bool, :default => false
  config_param :aggregate, :default => 'tag' do |val|
    case val
    when 'tag' then :tag
    when 'all' then :all
    else
      raise Fluent::ConfigError, "aggregate MUST be one of 'tag' or 'all'"
    end
  end
  config_param :input_tag_remove_prefix, :string, :default => nil
  config_param :monitor_key, :string
  config_param :output_key_prefix, :string, :default => nil
  config_param :percentiles, :default => nil do |val|
    values = val.split(",").map(&:to_i)
    if values.select{|i| i < 1 or i > 99 }.size > 0
      raise Fluent::ConfigError, "percentiles MUST be specified between 1 and 99 by integer"
    end
    values
  end

  config_param :samples_limit, :integer, :default => 1000000

  attr_accessor :count, :last_checked

  def configure(conf)
    super

    if @unit
      @count_interval = case @unit
                        when 'minute' then 60
                        when 'hour' then 3600
                        when 'day' then 86400
                        else
                          raise Fluent::ConfigError, "unit must be one of minute/hour/day"
                        end
    end

    if @input_tag_remove_prefix
      @removed_prefix_string = @input_tag_remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end

    @key_prefix_string = ''
    if @output_key_prefix
      @key_prefix_string = @output_key_prefix + '_'
    end

    if @output_per_tag
      raise Fluent::ConfigError, "tag_prefix must be specified with output_per_tag" unless @tag_prefix
      @tag_prefix_string = @tag_prefix + '.'
    end
    if @tag_prefix
      raise Fluent::ConfigError, "output_per_tag must be specified with tag_prefix" unless @output_per_tag
    end
    
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
      sleep 0.5
      if Fluent::Engine.now - @last_checked > @count_interval
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
        {'all' => {:min => nil, :max => nil, :sum => nil, :num => 0, :sample => []}}
      else
        {'all' => {:min => nil, :max => nil, :sum => nil, :num => 0}}
      end
    elsif keys
      values = if @percentiles
                 Array.new(keys.length) {|i| {:min => nil, :max => nil, :sum => nil, :num => 0, :sample => []}}
               else
                 Array.new(keys.length) {|i| {:min => nil, :max => nil, :sum => nil, :num => 0}}
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
    if count[:num] then output[key_prefix + 'num'] = count[:num] end
    if count[:min] then output[key_prefix + 'min'] = count[:min] end
    if count[:max] then output[key_prefix + 'max'] = count[:max] end
    if count[:num] > 0 then output[key_prefix + 'avg'] = (count[:sum] / (count[:num] * 1.0)) end
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
        Fluent::Engine.emit(@tag_prefix_string + tag, time, message)
      end
    else
      Fluent::Engine.emit(@tag, Fluent::Engine.now, flush)
    end
  end

  def countups(tag, min, max, sum, num, sample)
    if @aggregate == :all
      tag = 'all'
    end

    @mutex.synchronize do
      c = (@count[tag] ||= {:min => nil, :max => nil, :sum => nil, :num => 0})
      
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
