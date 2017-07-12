/**
 * Created by yuchef on 7/11/17.
 */
import org.apache.hadoop.util.hash.Hash;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonFactory;
import sun.security.krb5.Config;

import javax.management.StringValueExp;

import static java.util.logging.Level.CONFIG;

public class TestMain {
    public TestMain() throws IOException {
    }

    public static void main(String[] args) throws IOException{
        HashMap<String, Object> tuple;
        ParquetWriter<GenericRecord> writer;
       // Path path = new Path("dd");
        String name1 = ".sss.parquet.crc";
        String name2 = "sss.parquet";
        String name3 = "test.json";
        int n = 900000;
        File file1 = new File(name1);
        File file2 = new File(name2);
        File file3 = new File(name3);
        if (file1.exists() && file1.isFile()){
            file1.delete();
        }
        if (file2.exists() && file2.isFile()){
            file2.delete();
        }
        if (file3.exists() && file3.isFile()){
            file3.delete();
        }

        String path_string = TestMain.class.getResource("/").toString();
        path_string = path_string.substring(5);
        Path path = new Path("sss.parquet");
        List<Field> fileds= new ArrayList<Field>();
        fileds.add(new Field("Node1", Schema.create(Schema.Type.STRING), "", "sdw"));
        fileds.add(new Field("Node2", Schema.create(Schema.Type.INT), "", 1 ));
        fileds.add(new Field("Node3", Schema.create(Schema.Type.BOOLEAN), "", false ));
        fileds.add(new Field("Node4", Schema.create(Schema.Type.STRING), "", "sdw" ));
        fileds.add(new Field("Node5", Schema.create(Schema.Type.STRING), "", "sdw" ));
        Schema avroSchema = Schema.createRecord("Test", "","TestMain",false);
        avroSchema.setFields(fileds);
        WriteSupport<GenericRecord> writeSupport = new AvroWriteSupport<>(
                new AvroSchemaConverter().convert(avroSchema), avroSchema
        );
        Configuration configuration;
        configuration = new Configuration();
        System.out.println(avroSchema.toString());
        AvroWriteSupport.setSchema(configuration, avroSchema);
        System.out.println("Parquet write start" );
        long startTime = System.currentTimeMillis();
        writer = new ParquetWriter<GenericRecord>(path,writeSupport,CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE,true,true,ParquetWriter.DEFAULT_WRITER_VERSION);

        for (int i = 1; i < n; i++){
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("Node1",String.valueOf(i)+String.valueOf(i));
            record.put("Node2",i);
            if (i%2 == 0)
                record.put("Node3",false);
            else
                record.put("Node3",true);
            record.put("Node4",String.valueOf(i));
            record.put("Node5",String.valueOf(i)+String.valueOf(900000-i));
            try {
                writer.write(record);
            } catch (final IOException e){
                e.printStackTrace();
            }
        }
        writer.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Parquet write end");
        System.out.println(" Total time for Parquet writing: "+ (endTime - startTime)/1000.0);
        System.out.println("Parquet read start");
        startTime = System.currentTimeMillis();
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(new Configuration(), path);
        GenericRecord record;
        ArrayList<GenericRecord> result = new ArrayList<>();
        while ((record = reader.read())!= null){
            result.add(record);
            System.out.println(record.toString());
        }
        endTime = System.currentTimeMillis();
        System.out.println("Parquet read end");
        System.out.println(" Total time for Parquet reading: "+ (endTime - startTime)/1000.0);
        System.out.println(path_string);
        JsonFactory jsonFactory = new JsonFactory();
        System.out.println("Json write start" );
        startTime = System.currentTimeMillis();
        File jsonFile = new File("test.json");
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(jsonFile, JsonEncoding.UTF8);
        ObjectMapper mapper = new ObjectMapper();
        List<HashMap<String, Object>> input = new ArrayList<>();
        for (int i = 1; i < n; i++){
            HashMap<String, Object> data = new HashMap<>();
            data.put("Node1",String.valueOf(i)+String.valueOf(i));
            data.put("Node2",i);
            if (i%2 == 0)
                data.put("Node3",false);
            else
                data.put("Node3",true);
            data.put("Node4",String.valueOf(i));
            data.put("Node5",String.valueOf(i)+String.valueOf(900000-i));
            input.add(data);


        }
        HashMap<String,Object> ans = new HashMap<>();
        ans.put("result", input);
        mapper.writeValue(jsonFile,ans);
        endTime = System.currentTimeMillis();
        System.out.println("Json write end");
        System.out.println(" Total time for Json writing: "+ (endTime - startTime)/1000.0);

        System.out.println("Json read start" );
        startTime = System.currentTimeMillis();

        Map<String, Object> output = mapper.readValue(jsonFile, Map.class);
        ArrayList<HashMap<String, Object>> outlist = (ArrayList<HashMap<String, Object>>)output.get("result");
        endTime = System.currentTimeMillis();
        System.out.println("Json read end");
        System.out.println(" Total time for Json reading: "+ (endTime - startTime)/1000.0);

        System.out.println("Csv write start" );
        startTime = System.currentTimeMillis();
        File csvfile = new File("testfile.csv");
        csvfile.createNewFile();
        FileWriter fileWriter = new FileWriter(csvfile);
        for (int i = 0; i < n; i++){
            fileWriter.write(String.valueOf(i) + String.valueOf(i) + "," + String.valueOf(i)+","+"false"+"," + String.valueOf(i)
            +"," + String.valueOf(i)+String.valueOf(900000-i)+"\n");
        }
        fileWriter.close();
        endTime = System.currentTimeMillis();
        System.out.println("Csv write end");
        System.out.println(" Total time for Csv writing: "+ (endTime - startTime)/1000.0);

        char[] buf = new char[40];
        int count = 0;
        ArrayList<String[]> out = new ArrayList<>();
        String tempstring;
        System.out.println("Csv read start" );
        startTime = System.currentTimeMillis();
        FileReader fileReader = new FileReader(csvfile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        while ((tempstring = bufferedReader.readLine())!= null ){
            String[] tmp = tempstring.split(",");
            out.add(tmp);
        }
        fileReader.close();
        endTime = System.currentTimeMillis();
        System.out.println("Csv read end");
        System.out.println(" Total time for Csv reading: "+ (endTime - startTime)/1000.0);
        System.out.println(outlist.size() + out.size());
    }

}



