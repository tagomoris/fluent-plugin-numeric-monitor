# -*- encoding: utf-8 -*-
Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-numeric-monitor"
  gem.version       = "0.1.0"
  gem.authors       = ["TAGOMORI Satoshi"]
  gem.email         = ["tagomoris@gmail.com"]
  gem.description   = %q{Fluentd plugin to calculate min/max/avg/Xpercentile values, and emit these data as message}
  gem.summary       = %q{Fluentd plugin to calculate min/max/avg/Xpercentile values}
  gem.homepage      = "https://github.com/tagomoris/fluent-plugin-numeric-monitor"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency "fluentd"
  gem.add_development_dependency "rake"
  gem.add_runtime_dependency "fluentd"
end
