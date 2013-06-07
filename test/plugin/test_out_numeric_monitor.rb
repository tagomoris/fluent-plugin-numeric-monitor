require 'helper'

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

  def create_driver(conf = CONFIG, tag='test.input')
    Fluent::Test::OutputTestDriver.new(Fluent::NumericMonitorOutput, tag).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver CONFIG + %[
        output_per_tag true
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver CONFIG + %[
        tag_prefix prefix
      ]
    }
    #TODO
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
    d1 = create_driver(CONFIG, 'test.tag1')
    d1.run do
      10.times do
        d1.emit({'field1' => 0})
        d1.emit({'field1' => '1'})
        d1.emit({'field1' => 2})
        d1.emit({'field1' => '3'})
        d1.emit({'field1' => 4})
        d1.emit({'field1' => 5})
        d1.emit({'field1' => 6})
        d1.emit({'field1' => 7})
        d1.emit({'field1' => 8})
        d1.emit({'field1' => 9})
      end
    end
    r1 = d1.instance.flush
    assert_equal 0, r1['tag1_min']
    assert_equal 9, r1['tag1_max']
    assert_equal 4.5, r1['tag1_avg']
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
    ], 'test.tag1')

    d1.run do
      10.times do
        d1.emit({'field1' => 0})
        d1.emit({'field1' => '1'})
        d1.emit({'field1' => 2})
        d1.emit({'field1' => '3'})
        d1.emit({'field1' => 4})
        d1.emit({'field1' => 5})
        d1.emit({'field1' => 6})
        d1.emit({'field1' => 7})
        d1.emit({'field1' => 8})
        d1.emit({'field1' => 9})
      end
    end
    r1 = d1.instance.flush
    assert_equal 0, r1['min']
    assert_equal 9, r1['max']
    assert_equal 4.5, r1['avg']
    assert_equal 7, r1['percentile_80']
    assert_equal 8, r1['percentile_90']
    assert_equal 100, r1['num']
  end

  def test_without_percentiles
    d = create_driver(%[
      unit minute
      tag testmonitor
      monitor_key x1
    ], 'test')
    d.run do
      d.emit({'x1' => 1})
      d.emit({'x1' => 2})
      d.emit({'x1' => 3})
    end
    r = d.instance.flush
    assert_equal 1, r['test_min']
    assert_equal 3, r['test_max']
    assert_equal 2, r['test_avg']
    assert_equal 3, r['test_num']
  end

  def test_output_per_tag
    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag true
      tag_prefix tag_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 2, d.emits.size
    tag, r = d.emits[0][0], d.emits[0][2]
    assert_equal 'tag_prefix.tag1', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 3, r['num']
    tag, r = d.emits[1][0], d.emits[1][2]
    assert_equal 'tag_prefix.tag2', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 3, r['num']

    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag false
      tag output_tag
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag, r = d.emits[0][0], d.emits[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['tag1_min']
    assert_equal 3, r['tag1_max']
    assert_equal 2, r['tag1_avg']
    assert_equal 3, r['tag1_num']
    assert_equal 1, r['tag2_min']
    assert_equal 3, r['tag2_max']
    assert_equal 2, r['tag2_avg']
    assert_equal 3, r['tag2_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag true
      tag_prefix tag_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag = d.emits[0][0]
    r = d.emits[0][2]
    assert_equal 'tag_prefix.all', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 6, r['num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag false
      tag output_tag
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag = d.emits[0][0]
    r = d.emits[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['min']
    assert_equal 3, r['max']
    assert_equal 2, r['avg']
    assert_equal 6, r['num']
  end

  def test_output_key_prefix
    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag true
      tag_prefix tag_prefix
      output_key_prefix key_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 2, d.emits.size
    tag, r = d.emits[0][0], d.emits[0][2]
    assert_equal 'tag_prefix.tag1', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 3, r['key_prefix_num']
    tag, r = d.emits[1][0], d.emits[1][2]
    assert_equal 'tag_prefix.tag2', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 3, r['key_prefix_num']

    d = create_driver(CONFIG + %[
      aggregate tag
      output_per_tag false
      tag output_tag
      output_key_prefix key_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag, r = d.emits[0][0], d.emits[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['key_prefix_tag1_min']
    assert_equal 3, r['key_prefix_tag1_max']
    assert_equal 2, r['key_prefix_tag1_avg']
    assert_equal 3, r['key_prefix_tag1_num']
    assert_equal 1, r['key_prefix_tag2_min']
    assert_equal 3, r['key_prefix_tag2_max']
    assert_equal 2, r['key_prefix_tag2_avg']
    assert_equal 3, r['key_prefix_tag2_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag true
      tag_prefix tag_prefix
      output_key_prefix key_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag = d.emits[0][0]
    r = d.emits[0][2]
    assert_equal 'tag_prefix.all', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 6, r['key_prefix_num']

    d = create_driver(CONFIG + %[
      aggregate all
      output_per_tag false
      tag output_tag
      output_key_prefix key_prefix
    ], 'tag')
    d.run do
      d.tag = 'tag1'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
      d.tag = 'tag2'
      d.emit({'field1' => 1})
      d.emit({'field1' => 2})
      d.emit({'field1' => 3})
    end
    d.instance.flush_emit
    assert_equal 1, d.emits.size
    tag = d.emits[0][0]
    r = d.emits[0][2]
    assert_equal 'output_tag', tag
    assert_equal 1, r['key_prefix_min']
    assert_equal 3, r['key_prefix_max']
    assert_equal 2, r['key_prefix_avg']
    assert_equal 6, r['key_prefix_num']
  end
end
