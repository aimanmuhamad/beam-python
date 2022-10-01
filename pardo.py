import apache_beam as beam

class SplitRow(beam.DoFn):
    def process(self,element):
        customer = element.split(";")
        yield customer

class key_val(beam.DoFn):
    def process(self, element):
        return [element[2]+","+element[1],int(element[3])]
    
class filters(beam.DoFn):
    def process(self,element):
        print(element)
        if element[3] == 'Cash':
            return [element]

with beam.Pipeline() as pipe:
    ip = (pipe
        |beam.io.ReadFromText('cust.csv', skip_header_lines=True)
        |beam.ParDo(SplitRow())
        |beam.ParDo(filters())
        |beam.ParDo(key_val())
        |beam.CombinePerKey(sum)
        |beam.Map(print)
    )
