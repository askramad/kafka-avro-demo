import io.confluent.kafka.schemaregistry.tools.SchemaRegistryPerformance;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaProps.put("key.serializer", "io.confluent.kafka.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.KafkaAvroSerializer");

        kafkaProps.put("schema.registry.url","");

        Producer producer = new KafkaProducer<String,Customer>(kafkaProps);

        List<Customer> customerList = new ArrayList<Customer>();

        customerList.add(new Customer(1,"test","test",10));
        customerList.add(new Customer(2,"test","test",20));
        customerList.add(new Customer(2,"test","test",30));

        String schemaStream = "{\\namespace\\: \\customerManagement.avro\\," +
                " \\type\\: \\record\\, \\name\\: \\Customer\\, \\fields\\:" +
                " [ {\\name\\: \\customerId\\, \\type\\: \\int\\}, {\\name\\: \\firstName\\, " +
                "\\type\\: \\string\\}, {\\name\\: \\lastName\\, \\type\\: [\\null\\, \\string\\]," +
                "{\\name\\: \\age\\, \\type\\: \\int\\}, \\default\\: \\null\\} ] }";

        Schema.Parser schemaParser = new Schema.Parser();
        Schema schema = schemaParser.parse(schemaStream);

        GenericRecord customer = new GenericData.Record(schema);

        for(Customer c : customerList){

            customer.put("customerId",c.getCustomerId());
            customer.put("firstNme",c.getFirstName());
            customer.put("lastName",c.getLastName());
            customer.put("age",c.getAge());

            ProducerRecord <String,GenericRecord> data = new ProducerRecord<String,GenericRecord>("customers",String.valueOf(c.getCustomerId()),customer);

            producer.send(data);
        }





    }
}
