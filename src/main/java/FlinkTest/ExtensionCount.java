package FlinkTest;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

public class ExtensionCount {


	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> text = env.readTextFile("./data/log.json");
		DataSet<Tuple2<String, Integer>> counts =
				text.flatMap(new LineSplitter())
				.groupBy(0)
				.sum(1);
		counts.print();

	}

	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] lines = value.split("\\r?\\n");

			for (String line : lines) {
				try {
					JSONObject jsonObj = new JSONObject(line);
					String[] file = jsonObj.getString("nm").split("\\.");
					if(file.length > 1){
						out.collect(new Tuple2<String, Integer>(file[1], 1));
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
