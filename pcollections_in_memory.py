import apache_beam as beam

#inmemory data
a = [10,5,3]

with beam.Pipeline() as p:
    a1 = (p
          |beam.Create(a)
          |beam.Filter(lambda x:x>2)
          |beam.Map(print)
    )