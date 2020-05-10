import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

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
                return [{'id':element_id, 'city_county':city_county, 'province_state':province_state}]
        return [{'id':element_id, 'city_county':city_county, 'province_state':province_state}]
          
def run():
    PROJECT_ID = 'spry-cosine-266801' 
    options = {
     'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT id, province_state FROM covid19_jhu_csse_modeled.us_location_id limit 20'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    query_results | 'Write log 1' >> WriteToText('input.txt')
    
    # apply ParDo to format state  
    formatted_state_pcoll = query_results | 'Format State' >> beam.ParDo(FormatStateFn())

    # write PCollection to log file
    formatted_state_pcoll | 'Write log 2 for state' >> WriteToText('formatted_state_pcoll.txt')
     
    
    dataset_id = 'covid19_jhu_csse_modeled'
    table_id = 'us_location_id_Beam'
    schema_id = 'id:INTEGER, city_county:STRING, province_state:STRING'

    # write PCollection to new BQ table
    
    formatted_state_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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