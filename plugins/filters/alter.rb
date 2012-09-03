require "logstash/filters/base"
require "logstash/namespace"

# The alter filter allows you to do general mutations to fields 
# that are not included in the normal mutate filter. 
#
class LogStash::Filters::Alter < LogStash::Filters::Base
  config_name "alter"
  plugin_status "beta"
  
  # Change the content of the field to the specified value
  # if the actual content is equal to the expected one.
  #
  # Example:
  #
  #     filter {
  #       alter {
  #         condrewrite => [ 
  #              "field_name", "expected_value", "new_value" 
  #              "field_name2", "expected_value2, "new_value2"
  #              ....
  #            ]
  #       }
  #     }
  config :condrewrite, :validate => :array
  
  # Change the content of the field to the specified value
  # if the content of another field is equal to the expected one.
  #
  # Example:
  #
  #     filter {
  #       alter => {
  #         condrewriteother => [ 
  #              "field_name", "expected_value", "field_name_to_change", "value",
  #              "field_name2", "expected_value2, "field_name_to_change2", "value2",
  #              ....
  #         ]
  #       }
  #     }
  config :condrewriteother, :validate => :array
  
  public
  def register 
    @condrewrite_parsed = []
    @condrewrite.nil? or @condrewrite.each_slice(3) do |field, expected, replacement|
      if [field, expected, replacement].any? {|n| n.nil?}
        @logger.error("Invalid condrewrte configuration. condrewrite has to define 3 elements per config entry", :file => file, :expected => expected, :replacement => replacement)
        raise "Bad configuration, aborting."
      end
      @condrewrite_parsed << {
        :field        => field,
        :expected       => expected,
        :replacement  => replacement
      }
    end # condrewrite
    
     @condrewriteother.nil? or @condrewriteother.each_slice(4) do |field, expected, replacement_field, replacement_value|
      if [field, expected, replacement_field, replacement_value].any? {|n| n.nil?}
        @logger.error("Invalid condrewrteother configuration. condrewriteother has to define 4 elements per config entry", :file => file, :expected => expected, :replacement_field => replacementfield, :replacement_value => replacementvalue)
        raise "Bad configuration, aborting."
      end
      @condrewriteother_parsed << {
        :field        => field,
        :expected       => expected,
        :replacement_field  => replacement_field,
        :replacement_value => replacement_value
      }
    end # condrewriteother
    
  end # def register
  
  public
  def filter(event)
    return unless filter?(event)

    condrewrite(event) if @condrewrite
    condrewriteother(event) if @condrewriteother

    filter_matched(event)
  end # def filter
  
  private
  def condrewrite(event)
    @condrewrite_parsed.each do |config|
      field = config[:field]
      expected = config[:expected]
      replacement = config[:replacement]

      if event[field].is_a?(Array)
        event[field] = event[field].map do |v|
          if v == expected
            v = replacement
          else
            v
          end
        end
      else
        if event[field] == expected
          event[field] = replacement
        end
      end
    end # @gsub_parsed.each
  end # def gsub
  
  
end