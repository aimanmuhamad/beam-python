import apache_beam as beam

def filter_trans_type(trans, input_element):
    return input_element[3]==trans
def cap(elem):
    return elem[0],elem[1].title(), elem[2], elem[3]
def key_val(x):
    return (x[3],x[1]+","+str(x[2]))

class MyTransform(beam.PTransform):
    def expand(self, input_col):
        a = (
            input_col
                | 'capital' >> beam.Map(cap)
                | 'key_val' >> beam.Map(key_val)
        )
        
with beam.Pipeline() as pipe:
    input_collection = (
        pipe
        |beam.io.ReadFromText('cust.csv')
        |beam.Map(lambda x:x.split(","))
    )
    
    UPI = (
        input_collection
        |beam.Filter(lambda record: filter_trans_type('UPI', record))
        |'My trans for UPI' >> MyTransform()
        |'Writing results to UPI file' >> beam.io.WriteToText('MYUPI_Result')
    )
    
    cash = (
        input_collection
        |beam.Filter(lambda record: filter_trans_type('cash', record))
        |'My trans for cash' >> MyTransform()
        |'Writing results to UPI file' >> beam.io.WriteToText('MYcash_Result')
    )
