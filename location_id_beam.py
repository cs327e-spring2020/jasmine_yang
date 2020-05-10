import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

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
    options = {
     'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT id, province_state, country_region FROM covid19_jhu_csse_modeled.location_id where country_region = "US" limit 20'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    query_results | 'Write log 1' >> WriteToText('input.txt')
    
    # apply ParDo to format US
    formatted_us_pcoll = query_results | 'Format US' >> beam.ParDo(FormatUSFn())

    # write PCollection to log file
    formatted_us_pcoll | 'Write log 2 for US' >> WriteToText('formatted_us_pcoll.txt')
     
    
    dataset_id = 'covid19_jhu_csse_modeled'
    table_id = 'location_id_Beam'
    schema_id = 'id:INTEGER, province_state:STRING, country_region:STRING'

    # write PCollection to new BQ table
    
    formatted_us_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()