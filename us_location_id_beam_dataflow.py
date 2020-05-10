import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatStateFn(beam.DoFn):
    def process(self, element):
        states_abv = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'}

        cusa_record = element
        # print(cusa_record)
        element_id = cusa_record.get('id')
        province_state = cusa_record.get('province_state')
        country_region = cusa_record.get('country_region')
                
        city_county = None
        state = None
        if province_state is not None:
            split_state = province_state.split(', ')
            if len(split_state) > 1:
                city_county = split_state[0]
                state = split_state[1]
                province_state = states_abv.get(state)
                print('city_county', city_county)
                print('state', state)
                return [{'id':element_id, 'city_county':city_county, 'province_state':province_state, 'country_region':country_region}]
        return [{'id':element_id, 'city_county':city_county, 'province_state':province_state, 'country_region':country_region}]
        
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
    google_cloud_options.job_name = 'us-location-df' # dataflow does not like '_' or special characters
    google_cloud_options.staging_location = BUCKET + '/staging' #req*
    google_cloud_options.temp_location = BUCKET + '/temp' #req*
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT id, province_state, country_region FROM covid19_jhu_csse_modeled.us_location_id'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # format state
    formatted_state_pcoll = query_results | 'Format State' >> beam.ParDo(FormatStateFn())

    # write PCollection to log file
    formatted_state_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'formatted_state_pcoll.txt')
    
    
    dataset_id = 'covid19_jhu_csse_modeled'
    table_id = 'us_location_id_Beam_DF'
    schema_id = 'id:INTEGER, city_county:STRING, province_state:STRING, country_region:STRING'

    # write PCollection to new BQ table
    formatted_state_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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