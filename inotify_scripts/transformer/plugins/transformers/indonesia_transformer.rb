module Transformers
  class IndonesiaTransformer < BaseTransformer
    register :indonesia

    configure(
      lines_to_ignore: 26,
      metadata_range: [2, 22],
      # patient_id,patient_status,registration_date,death_date,facility,region
      patient_columns: {
        patient_id: 4,
        patient_status: 'alive',
        registration_date: 1, # Assume "Date" is registration date
        death_date: nil,
        facility: 15, # Using "Ward" here as facility
        region: 15, # Using "Ward" here as region
      },
      # encounter_id,patient_id,encounter_date,diastolic_bp,systolic_bp
      bp_columns: {
        encounter_id: 0,
        patient_id: 4,
        encounter_date: 14, # Using the "Examination Date" here
        diastolic_bp: 55,
        systolic_bp: 56,
      }
    )
  end
end
