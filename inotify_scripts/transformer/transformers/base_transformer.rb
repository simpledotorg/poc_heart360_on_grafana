require 'csv'
require_relative '../capabilities/configurable'

module Transformers
  class BaseTransformer
    include Configurable

    # TODO: Pre/Post Extraction Hook-- something like "include PostProcessor"
    # There are some columns which need to be further processed before we
    # consider it to be the state we want it in. While it's easy to hardcode
    # this transform from the source format to our destination format, we
    # cannot always guarantee the source format. The best way here would be to
    # enable the downstream plugins to define how they want to transform their
    # source to our destination. Since this is too much of an endeavour to
    # complete in the time we have, I am leaving this as a to-do.
    #
    # Some potential usecases for this are
    # BP from "[num] [unit]" to "[num]"
    # Dates from "[date] [time]" to "[date]"

    @registry = {}

    def self.register(name)
      Transformers::BaseTransformer.registry[name] = self
    end

    def self.registry
      @registry
    end

    def self.find(name)
      @registry[name] || raise(ArgumentError, "Unknown template: #{name}")
    end

    attr_reader :filepath

    def initialize(filepath, template: :indonesia)
      @filepath = filepath
      parse_data
      parse_metadata
    end

    # This is a simple function which should read the CSV into memory. It
    # generates the object from which we would extract the different CSVs which
    # would load the DB tables
    def parse_data
      @data = CSV.read(@filepath).drop(config.lines_to_ignore)
    end

    # Some CSVs may come with metadata (i.e. some aspects in the data which
    # describe either how the data was generated, or how the data should be
    # parsed). This is important information for the parsing and should be
    # grokked. This function parses out the metadata in case we need it
    def parse_metadata
      data = CSV.read(@filepath)
      starts_at, num_items = config.metadata_range
      @metadata = data[starts_at, num_items].map(&:first)
    end

    # This properly identifies the file we are parsing. The current
    # identification format is "[health centre]-[date range]"
    def id
      if @metadata.nil?
        parse_metadata
      end

      @metadata[0, 2].map! { |m| m.split(':').last.strip }.join('_').gsub(' ', '_')
    end

    # Generate the patient data from the CSV being parsed based on the
    # configuration#patient_columns.
    #
    # The output can be configured by environment variables
    def patients_data
      outfile = ENV.fetch("PATIENTS_OUT", "patients.csv")
      extract :patient_columns, outfile
    end

    # Generate the patient data from the CSV being parsed based on the
    # configuration#bp_columns.
    #
    # The output can be configured by environment variables
    def bp_data
      outfile = ENV.fetch("BP_OUT", "bp_encounters.csv")
      extract :bp_columns, outfile
    end

    private

    def extract target, outfile
      headers = config.send(target).keys.map(&:to_s)
      indexes = config.send(target).values
      CSV.open(outfile, "wb") do |csv|
        csv << headers
        @data.each do |row|
          csv << indexes.map do |i|
            if i.is_a? Integer
              row[i]
            else
              i
            end
          end
        end
      end
    end
  end
end

#  A 0  No.
#  B 1  Date
#  C 2  Patient Name
#  D 3  No. eRM
#  E 4  NIK
#  F 5  Family Card No.
#  G 6  Old RM No.
#  H 7  RM Document No.
#  I 8  Gender
#  J 9  Phone number
#  K 10 Address
#  L 11 RT
#  M 12 RW
#  N 13 Work
#  O 14 Examination Date
#  P 15 Ward
#  Q 16 Place of birth
#  R 17 Date of Birth
#  S 18 Age in years
#  T 19 Age in months
#  U 20 Age in days
#  V 21 Father's name
#  W 22 Mother's Name
#  X 23 Type of Visit
#  Y 24 Poly/Room
#  Z 25 Insurance
# AA 26 Insurance No.
# AB 27 Abnormalities
# AC 28 Doctor / Medical Personnel
# AD 29 Nurse / Midwife / Nutritionist / Sanitarian
# AE 30 SOAP Assessment
# AF 31 SOAP Subjective
# AG 32 SOAP Objective
# AH 33 SOAP Planning
# AI 34 Main Complaint
# AJ 35 Additional Complaints
# AK 36 Length of Illness
# AL 37 Smoke
# AM 38 Alcohol Consumption
# AN 39 Lack of Vegetables/Fruit
# AO 40 Therapy
# AP 41 Education
# AQ 42 Nursing Actions
# AR 43 Information
# AS 44 RPS
# AT 45 RPD
# AU 46 RPK
# AV 47 Allergies
# AW 48 Awareness
# AX 49 Triage
# AY 50 Tall
# AZ 51 Weight
# BA 52 Abdominal Circumference
# BB 53 BMI
# BC 54 BMI results
# BD 55 Systole
# BE 56 Diastole
# BF 57 Breath
# BG 58 Pulse
# BH 59 Heart rate
# BI 60 Temperature
# BJ 61 Physical Activity and Functional Assessment
# BK 62 Pain Scale
# BL 63 ICD-X 1
# BM 64 Diagnosis 1
# BN 65 Case Type 1
# BO 66 ICD-X 2
# BP 67 Diagnosis 2
# BQ 68 Case Type 2
# BR 69 ICD-X 3
# BS 70 Diagnosis 3
# BT 71 Case Type 3
# BU 72 ICD-X 4
# BV 73 Diagnosis 4
# BW 74 Case Type 4
# BX 75 ICD-X 5
# BY 76 Diagnosis 5
# BZ 77 Case Type 5
# CA 78 Action
# CB 79 Recipe
# CC 80 Pharmacist
# CD 81 Internal Registration/Referral
# CE 82 Long Queue
# CF 83 Examination Time
# CG 84 Duration of Drug Service
# HA 85 Registration Officer
