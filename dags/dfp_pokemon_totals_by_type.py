import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='proud-limiter-422923-k4',
    region='us-central1',
    staging_location='gs://us-central1-cmp-sandbox-dev-e1950a8d-bucket/staging',
    temp_location='gs:/us-central1-cmp-sandbox-dev-e1950a8d-bucket/temp'
)


def parse_pokemon_csv(line):
    import csv
    if not line.strip():
        return None

    try:
        if parse_pokemon_csv.header is None:
            parse_pokemon_csv.header = line
            return None

        reader = csv.DictReader([parse_pokemon_csv.header, line])
        for row in reader:
            return {
                'id': int(row['id']),
                'name': row['name'],
                'type': int(row['type']),
                'type_name': row['type_name'],
                'height': float(row['height']),
                'weight': float(row['weight'])
            }
    except (csv.Error, ValueError) as e:
        print(f"Error parsing CSV line: {e}")
        return None


parse_pokemon_csv.header = None


def run_pipeline():
    with beam.Pipeline(options=pipeline_options) as pipeline:
        poke_data = (
                pipeline
                | 'ReadFromGCS' >> beam.io.ReadFromText('gs://us-central1-cmp-sandbox-dev-e1950a8d-bucket/data/pokemon.csv')
                | 'ParseCSV' >> beam.Map(parse_pokemon_csv)
                | 'FilterNulls' >> beam.Filter(lambda x: x is not None)
                | "FilterPokemonTypeName" >> beam.Filter(lambda pokemon: 'type' in pokemon and 'type_name' in pokemon)
        )

        format_data = (
                poke_data
                | "ExtractPokemonType" >> beam.Map(lambda pokemon: (pokemon['type'], pokemon['type_name']))
                | "GroupByPokemonType" >> beam.CombinePerKey(beam.combiners.ToListCombineFn())
                | "Formatted Data" >> beam.Map(
            lambda tipo_conteo: {'type_name': tipo_conteo[1][0], 'total': len(tipo_conteo[1])})
        )

        format_data | beam.Map(print)

        format_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table='proud-limiter-422923-k4.pokemons.totals_by_type',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )


if __name__ == '__main__':
    run_pipeline()
