import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

/**
 * Created by dlo on 15/05/17.
 */
public class InputReadAvro {
    public static void main(String[] args) throws IOException{
        String fileName = args[0];
        File file = new File(fileName);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
        while(dataFileReader.hasNext()) {
            GenericRecord result = dataFileReader.next();
            String output = String.format("%s\t%s\t%s\t%s",result.get("sighting_date"),
                    result.get(1), result.get("shape"), result.get("duration"));
            System.out.println(output);
        }
    }
}
