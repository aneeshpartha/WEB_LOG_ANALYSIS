// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.String;

public class weblogMapper
  extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   String line = value.toString();

 //String keys = line.substring(0,7);
//int linkstart = line.indexOf("/");
//int linkend = line.indexOf(" -",linkstart);

//String url = line.substring(linkstart,linkend-1);


// To filter the records based on the given condition.

if(!line.startsWith("#")){
if(!line.contains("index.")==true){

if ( line.contains(" 200 ") == true) {

 String keys = line.substring(0,7);
int linkstart = line.indexOf("/");// Start of cs_uri_stem field
int linkend = line.indexOf(" ",linkstart); // End of cs_uri_stem_field
String url = line.substring(linkstart,linkend); //Cutting the data and taking it out.
context.write(new Text(keys),new Text(url));

}

}
}
    }
  }



