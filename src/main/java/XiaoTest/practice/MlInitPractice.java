package XiaoTest.practice;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/** 
* @author Lusx 
* @date 2019年11月21日 上午11:02:27 
*/
public class MlInitPractice {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.master("local")
				.config("spark.sql.crossJoin.enabled", "true")
				//.enableHiveSupport()
				.getOrCreate();
		

		// Prepare training data.
		/*List<Row> dataTraining = Arrays.asList(
		    RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
		    RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
		    RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
		    RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
		);
		
		
		StructType schema = new StructType(new StructField[]{
		    new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
		    new StructField("features", new VectorUDT(), false, Metadata.empty())
		});
		Dataset<Row> training = spark.createDataFrame(dataTraining, schema);

		// Create a LogisticRegression instance. This instance is an Estimator.
		LogisticRegression lr = new LogisticRegression();
		// Print out the parameters, documentation, and any default values.
		System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");

		// We may set parameters using setter methods.
		lr.setMaxIter(10).setRegParam(0.01);

		// Learn a LogisticRegression model. This uses the parameters stored in lr.
		LogisticRegressionModel model1 = lr.fit(training);
		// Since model1 is a Model (i.e., a Transformer produced by an Estimator),
		// we can view the parameters it used during fit().
		// This prints the parameter (name: value) pairs, where names are unique IDs for this
		// LogisticRegression instance.
		System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

		// We may alternatively specify parameters using a ParamMap.
		ParamMap paramMap = new ParamMap()
		  .put(lr.maxIter().w(20))  // Specify 1 Param.
		  .put(lr.maxIter(), 30)  // This overwrites the original maxIter.
		  .put(lr.regParam().w(0.1), lr.threshold().w(0.55));  // Specify multiple Params.

		// One can also combine ParamMaps.
		ParamMap paramMap2 = new ParamMap()
		  .put(lr.probabilityCol().w("myProbability"));  // Change output column name
		ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

		// Now learn a new model using the paramMapCombined parameters.
		// paramMapCombined overrides all parameters set earlier via lr.set* methods.
		LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
		System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

		// Prepare test documents.
		List<Row> dataTest = Arrays.asList(
		    RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
		    RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
		    RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))
		);
		Dataset<Row> test = spark.createDataFrame(dataTest, schema);

		// Make predictions on test documents using the Transformer.transform() method.
		// LogisticRegression.transform will only use the 'features' column.
		// Note that model2.transform() outputs a 'myProbability' column instead of the usual
		// 'probability' column since we renamed the lr.probabilityCol parameter previously.
		Dataset<Row> results = model2.transform(test);
		Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
		for (Row r: rows.collectAsList()) {
		  System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
		    + ", prediction=" + r.get(3));
		}*/
		
		//--------------------------------------------------Pipeline--------------------------------------------------------------------------
		
		List<Row> listRow = Arrays.asList(
			    RowFactory.create(0L, "a b c d e spark", 1.0),
			    RowFactory.create(1L, "b d", 0.0),
			    RowFactory.create(2L, "spark f g h", 1.0),
			    RowFactory.create(3L, "hadoop mapreduce", 0.0)
			);
		
		StructType schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, false, Metadata.empty()), 
				new StructField("text", DataTypes.StringType, false, Metadata.empty()), 
			    new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
			});
		
		Dataset<Row> training = spark.createDataFrame(listRow,schema);
		
		// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
		Tokenizer tokenizer = new Tokenizer()
		  .setInputCol("text")
		  .setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
		  .setNumFeatures(1000)
		  .setInputCol(tokenizer.getOutputCol())
		  .setOutputCol("features");
		LogisticRegression lr = new LogisticRegression()
		  .setMaxIter(10)
		  .setRegParam(0.001);
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

		// Fit the pipeline to training documents.
		PipelineModel model = pipeline.fit(training);

		// Prepare test documents, which are unlabeled.ngx
		List<Row> listRow2 = Arrays.asList(
			    RowFactory.create(4L, "spark i j k"),
			    RowFactory.create(5L, "l m n"),
			    RowFactory.create(6L, "spark hadoop spark"),
			    RowFactory.create(7L, "apache hadoop")
			);
		StructType schema2 = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, false, Metadata.empty()), 
				new StructField("text", DataTypes.StringType, false, Metadata.empty()) 
			});
		Dataset<Row> test = spark.createDataFrame(listRow2 , schema2);

		// Make predictions on test documents.
		Dataset<Row> predictions = model.transform(test);
		for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
		  System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
		    + ", prediction=" + r.get(3));
		}
	}
}
