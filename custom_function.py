import apache_beam as beam

your_data = YOUR_DATA

def filter_data(element):
    if element[16]=='9':
        return element

with beam.Pipeline() as pipe1:
      ip1 = (pipe1
      |beam.io.ReadFromText(your_data, skip_header_lines=False)
      |beam.Map(maps)
      |beam.Filter(filter_data)
      |beam.combiners.Count.Globally()
      |beam.Map(print)
     )
