import apache_beam as beam

with beam.Pipeline() as pipeline:
  max_element = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get max value' >>
      beam.CombineGlobally(lambda elements: max(elements or [None]))
      | beam.Map(print))

with beam.Pipeline() as pipeline:
  min_element = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get min value' >>
      beam.CombineGlobally(lambda elements: min(elements or [-1]))
      | beam.Map(print))
  