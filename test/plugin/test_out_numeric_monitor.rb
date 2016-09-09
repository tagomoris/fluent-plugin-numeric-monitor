require 'helper'
require 'fluent/test/driver/output'

class NumericMonitorOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    unit minute
    tag monitor.test
    input_tag_remove_prefix test
    monitor_key field1
    percentiles 80,90
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::NumericMonitorOutput).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      create_driver CONFIG + %[
        output_per_tag true
      ]
    }
    assert_raise(Fluent::ConfigError) {
      create_driver CONFIG + %[
        tag_prefix prefix
      ]
    }
    d = create_driver(CONFIG)
    assert_equal(60, d.instance.count_interval)
    assert_equal(60, d.instance.unit)
    assert_equal("monitor.test", d.instance.tag)
    assert_equal(0.5, d.instance.interval)
    assert_nil(d.instance.tag_prefix)
    assert_false(d.instance.output_per_tag)
    assert_equal("tag", d.instance.aggregate)
    assert_equal("test", d.instance.input_tag_remove_prefix)
    assert_equal("field1", d.instance.monitor_key)
    assert_equal([80, 90], d.instance.percentiles)
  end

  def test_count_initialized
    #TODO
  end

  def test_countups
    #TODO
  end

  def test_stripped_tag
    d = create_driver
    assert_equal 'input', d.instance.stripped_tag('test.input')
    assert_equal 'test.input', d.instance.stripped_tag('test.test.input')
    assert_equal 'input', d.instance.stripped_tag('input')
  end

  def test_generate_output
    #TODO
  end

  def test_emit
    d1 = create_driver(CONFIG)
    d1.run(default_tag: 'test.tag1') do
      10.times do
        d1.feed({'field1' => 0})
        d1.feed({'field1' => '1'})
        d1.feed({'field1' => 2})
        d1.feed({'field1' => '3'})
        d1.feed({'field1' => 4})
        d1.feed({'field1' => 5})
        d1.feed({'field1' => 6})
        d1.feed({'field1' => 7})
        d1.feed({'field1' => 8})
        d1.feed({'field1' => 9})
      end
    end
    r1 = d1.instance.flush
    assert_equal 0, r1['tag1_min']
    assert_equal 9, r1['tag1_max']
    assert_equal 4.5, r1['tag1_avg']
    assert_equal 450, r1['tag1_sum']
    assert_equal 7, r1['tag1_percentile_80']
    assert_equal 8, r1['tag1_percentile_90']
    assert_equal 100, r1['tag1_num']
  end

  def test_emit_aggregate_all
    d1 = create_driver(%[
      unit minute
      tag monitor.test
      input_tag_remove_prefix test
      aggregate all
      monitor_key field1
      percentiles 80,90
    ])

    d1.run(default_tag: 'test.tag1') do
      10.times do
        d1.feed({'field1' => 0})
        d1.feed({'field1' => '1'})
        d1.feed({'field1' => 2})
        d1.feed({'field1' => '3'})
        d1.feed({'field1' => 4})
        d1.feed({'field1' => 5})
        d1.feed({'field1' => 6})
        d1.feed({'field1' => 7})
        d1.feed({'field1' => 8})
        d1.feed({'field1' => 9})
      end
    end
    r1 = d1.instance.flush
    assert_equal 0, r1['min']
    assert_equal 9, r1['max']
    assert_equal 4.5, r1['avg']
    assert_equal 450, r1['sum']
    assert_equal 7, r1['percentile_80']
    assert_equal 8, r1['percentile_90']
    assert_equal 100, r1['num']
  end

  def test_without_percentiles
    d = create_driver(%[
      unit minute
      tag testmonitor
      monitor_key x1
    ])
    d.run(default_tag: 'test') do
      d.feed({'x1' => 1})
      d.feed({'x1' => 2})
      d.feed({'x1' => 3})
    end
    r = d.instance.flush
    assert_equal 1, r['test_min']
    assert_equal 3, r['test_max']
    assert_equal 2, r['test_avg']
    assert_equal 6, r['test_sum']
    assert_equal 3, r['test_num']
  end

  def test_output_per_tag
    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag true
      tag_prefix tag_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 2, d.events.size
    tag, r = d.events[0][0], d.events[0][2]
    assert_equal 'tag_prefix.tag1', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 6, r['sum']
    assert_equal 3, r['num']
    tag, r = d.events[1][0], d.events[1][2]
    assert_equal 'tag_prefix.tag2', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 6, r['sum']
    assert_equal 3, r['num']

    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag false
      tag output_tag
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag, r = d.events[0][0], d.events[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['tag1_min']
    assert_equal 3, r['tag1_max']
    assert_equal 2, r['tag1_avg']
    assert_equal 6, r['tag1_sum']
    assert_equal 3, r['tag1_num']
    assert_equal 1, r['tag2_min']
    assert_equal 3, r['tag2_max']
    assert_equal 2, r['tag2_avg']
    assert_equal 6, r['tag1_sum']
    assert_equal 3, r['tag2_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag true
      tag_prefix tag_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag = d.events[0][0]
    r = d.events[0][2]
    assert_equal 'tag_prefix.all', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 12, r['sum']
    assert_equal 6, r['num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag false
      tag output_tag
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag = d.events[0][0]
    r = d.events[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 12, r['sum']
    assert_equal 6, r['num']
  end

  def test_output_key_prefix
    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag true
      tag_prefix tag_prefix
      output_key_prefix key_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 2, d.events.size
    tag, r = d.events[0][0], d.events[0][2]
    assert_equal 'tag_prefix.tag1', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 6, r['key_prefix_sum']
    assert_equal 3, r['key_prefix_num']
    tag, r = d.events[1][0], d.events[1][2]
    assert_equal 'tag_prefix.tag2', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 6, r['key_prefix_sum']
    assert_equal 3, r['key_prefix_num']

    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag false
      tag output_tag
      output_key_prefix key_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag, r = d.events[0][0], d.events[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['key_prefix_tag1_min']
    assert_equal 3, r['key_prefix_tag1_max']
    assert_equal 2, r['key_prefix_tag1_avg']
    assert_equal 6, r['key_prefix_tag1_sum']
    assert_equal 3, r['key_prefix_tag1_num']
    assert_equal 1, r['key_prefix_tag2_min']
    assert_equal 3, r['key_prefix_tag2_max']
    assert_equal 2, r['key_prefix_tag2_avg']
    assert_equal 6, r['key_prefix_tag2_sum']
    assert_equal 3, r['key_prefix_tag2_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag true
      tag_prefix tag_prefix
      output_key_prefix key_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag1') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag = d.events[0][0]
    r = d.events[0][2]
    assert_equal 'tag_prefix.all', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 12, r['key_prefix_sum']
    assert_equal 6, r['key_prefix_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag false
      tag output_tag
      output_key_prefix key_prefix
    ])
    time = Time.now.to_i
    d.run(default_tag: 'tag') do
      tag1 = 'tag1'
      d.feed(tag1, time, {'field1' => 1})
      d.feed(tag1, time, {'field1' => 2})
      d.feed(tag1, time, {'field1' => 3})
      tag2 = 'tag2'
      d.feed(tag2, time, {'field1' => 1})
      d.feed(tag2, time, {'field1' => 2})
      d.feed(tag2, time, {'field1' => 3})
      d.instance.flush_emit
    end
    assert_equal 1, d.events.size
    tag = d.events[0][0]
    r = d.events[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 12, r['key_prefix_sum']
    assert_equal 6, r['key_prefix_num']
  end
end
