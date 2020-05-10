import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatUSFn(beam.DoFn):
    def process(self, element):
        location_record = element
        # print(location_record)
        element_id = location_record.get('id')
        province_state = location_record.get('province_state')
        country_region = location_record.get('country_region')
        
        if country_region is not None:
            if country_region == 'US':
                location_record['country_region'] = 'United States'
        location_tuple = (element_id, location_record)
        return [location_record]
        
def run():         
    PROJECT_ID = 'spry-cosine-266801' 
    BUCKET = 'gs://icyhot-pack_beam' 
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'location-df' # dataflow does not like '_' or special characters
    google_cloud_options.staging_location = BUCKET + '/staging' #req*
    google_cloud_options.temp_location = BUCKET + '/temp' #req*
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT id, province_state, country_region FROM covid19_jhu_csse_modeled.location_id'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # format US
    formatted_us_pcoll = query_results | 'Format US' >> beam.ParDo(FormatUSFn())

    # write PCollection to log file
    formatted_us_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'formatted_us_pcoll.txt')
    
    
    dataset_id = 'covid19_jhu_csse_modeled'
    table_id = 'location_id_Beam_DF'
    schema_id = 'id:INTEGER, province_state:STRING, country_region:STRING'

    # write PCollection to new BQ table
    formatted_us_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
         
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()