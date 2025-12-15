module Configurable
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    # This is a default configuration which holds what can be configured in a
    # particular parser. If the key does not exist here, it may not be set in
    # the parsers downstream. The format of the definition is
    #
    # - lines_to_ignore: int
    # - metadata_range: int (start), int (length)
    # - patient_columns: hash of `[output column name]: [input column index]`
    # - bp_columns: hash of `[output column name]: [input column index]`
    #
    # ...where the "input" is the file being parsed, and the "output" are the
    # different CSVs needed to load in the data into the database.
    #
    def config_struct
      @config_struct ||= Struct.new(
        :lines_to_ignore,
        :metadata_range,
        :patient_columns,
        :bp_columns,
      )
    end

    # This is the class method which would be used by plugin developers to
    # configure their plugins. The keys are defined in `default_config`
    def configure(**options)
      @configuration = config_struct.new(**default_config.merge(options))
    end

    def configuration
      @configuration || configure
    end

    # NOTE: This method must be overriden in the child class by calling the
    # `configure` class method, else you would get bad data in the parsing. If
    # you read this closely, you'd realize the columns being extracted as
    # `patient_columns` and `bp_columns` are the same, while their keys are
    # different. The same column cannot carry two different meanings. This is
    # kept like so in the default to force the plugin developers (people who
    # write transformers for different data formats) to define their own
    # configuration for their own format.
    def default_config
      {
        lines_to_ignore: 0,
        metadata_range: [0, 0],
        patient_columns: {
          id: 0,
          date: 1,
          name: 2
        },
        bp_columns: {
          id: 0,
          date: 1,
          systole: 2
        }
      }
    end
  end

  def config
    self.class.configuration
  end
end
