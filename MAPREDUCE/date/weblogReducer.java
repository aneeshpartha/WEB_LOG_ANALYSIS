// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

public class weblogReducer
  extends Reducer<Text, Text , Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

ArrayList<String> list = new ArrayList<String>(); 
String link; 
for(Text value : values){
link=value.toString();

if(list.contains(link)!=true){
list.add(link);
}

}

int size=list.size();
int[] count = new int[size];

for(Text value : values){
link=value.toString();

for(int i=0; i < list.size() ; i++) {

if( link.compareTo(list.get(i))==0){

count[i]++;

}
}

}

int max=count[0];
int id=0;

for(int j=1;j<size;j++){

if(max < count[j]){

max = count[j];
id=j;

}
}

context.write(key , new Text(list.get(id)));

}


}       
// ^^ MaxTemperatureReducer
