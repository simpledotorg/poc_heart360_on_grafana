#!/usr/bin/env ruby

require_relative 'transformers/base_transformer'
Dir[File.join(__dir__, 'plugins', '**', '*.rb')].each { |f| require f }

# TODO: Specify locations for outfiles
# Right now, the out file is generated in the same folder as this script. This
# is the same as the original python script which puts the outfile in some
# hardcoded location. What we want is to be able to tell this script to put the
# outfile in a location of our choosing.

begin
  klass = Transformers::BaseTransformer.find(:indonesia)
  inputfile = ENV["INPUT_FILE"]
  raise "No input file" if inputfile.nil?
  transformer = klass.new(inputfile)
  puts "Using Transformer: #{transformer.class}"
  transformer.patients_data
  transformer.bp_data
rescue ArgumentError => e
  puts e.message
end
