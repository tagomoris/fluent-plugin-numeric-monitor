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
end
