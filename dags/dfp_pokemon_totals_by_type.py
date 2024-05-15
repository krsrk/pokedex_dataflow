import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='proud-limiter-422923-k4',
    region='us-central1',
    staging_location='gs://us-central1-cmp-sandbox-dev-e1950a8d-bucket/staging',
    temp_location='gs:/us-central1-cmp-sandbox-dev-e1950a8d-bucket/temp'
)


def run_pipeline():
    with beam.Pipeline(options=pipeline_options) as pipeline:
        poke_data = (
                pipeline
                | 'ReadFromGCS' >> beam.io.ReadFromText('gs://us-central1-cmp-sandbox-dev-e1950a8d-bucket/data/pokemon.csv')
        )

        '''result_set = (
                poke_data
                | "Filter by Pokemon Type Name" >> beam.Filter(
                                                        lambda pokemon: 'type' in pokemon and 'type_name' in pokemon
                                                   )
                | "Extract Pokemon Type" >> beam.Map(lambda pokemon: (pokemon['type'], pokemon['type_name']))
                | "Group by Pokemon Type" >> beam.CombinePerKey(beam.combiners.ToListCombineFn())
                | "Formatted Data" >> beam.Map(
                                            lambda tipo_conteo: f"type_name: {tipo_conteo[1][0]}, total: {str(len(tipo_conteo[1]))}"
                                      )
        )'''

        filter_data = (
                poke_data
                | "FilterPokemonTypeName" >> beam.Filter(lambda pokemon: 'type' in pokemon and 'type_name' in pokemon)
        )

        extracted_data = (
                filter_data
                | "ExtractPokemonType" >> beam.Map(lambda pokemon: (pokemon['type'], pokemon['type_name']))
        )

        group_data = (
                extracted_data
                | "GroupByPokemonType" >> beam.CombinePerKey(beam.combiners.ToListCombineFn())
        )

        formated_data = (
                group_data
                | "Formatted Data" >> beam.Map(
            lambda tipo_conteo: f"type_name: {tipo_conteo[1][0]}, total: {str(len(tipo_conteo[1]))}")
        )

        formated_data | "PrintFormattedData" >> beam.Map(print)
        print(formated_data)

        formated_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table='proud-limiter-422923-k4.pokemons.totals_by_type',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )

        print('Pokemon Data imported Successfully!')


if __name__ == '__main__':
    run_pipeline()
