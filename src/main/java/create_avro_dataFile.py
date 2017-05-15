import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumReader, DatumWriter

schema = avro.schema.Parse(open("ufo.avsc").read())
writer = DataFileWriter(open("ufo.avro", "wb"), DatumWriter(), schema)
writer.append({"sighting_date": "2012-01-12", "city": "Boston", "shape": "diamond", "duration" : 3.5})
writer.append({"sighting_date" : "2011-06-13", "city": "London", "shape": "light", "duration":13})
writer.append({"sighting_date" : "1999-12-31", "city" : "New York", "shape" : "light", "duration": 0.25})
writer.append({"sighting_date" : "2001-08-23", "city" : "Las Vegas", "shape" : "cylinder", "duration" : 1.2})
writer.append({"sighting_date" : "1975-11-09", "city" : "Miami", "duration" : 5})
writer.append({"sighting_date" : "2003-02-27", "city" : "Paris", "shape" : "light", "duration" : 0.5})
writer.append({"sighting_date" : "2007-04-12", "city" : "Dallas", "shape" : "diamond", "duration" : 3.5})
writer.append({"sighting_date" : "2009-10-10", "city" : "Milan", "shape" : "formation", "duration" : 0})
writer.append({"sighting_date" : "2012-04-10", "city" : "Amsterdam", "shape" : "blur", "duration" : 6})
writer.append({"sighting_date" : "2006-06-15", "city" : "Minneapolis", "shape" : "saucer", "duration" : 0.25})
writer.close()

