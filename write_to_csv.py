import apache_beam as beam

your_data = 'tomato_dataset.csv'

with beam.Pipeline() as pipe1:
      ip1 = (pipe1
      |beam.io.ReadFromText(your_data, skip_header_lines=False)
      |beam.Map(lambda x:x.split(","))
      |beam.Filter(lambda x:x[16]=='9')
      |beam.combiners.Count.Globally()
      |beam.io.WriteToText('write_to_text.csv')
     )