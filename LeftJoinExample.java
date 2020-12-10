package section5;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

//the left join returns all records from the left table (order) and the matched records from the right table (use)


class LeftOrderParsing extends DoFn<String,KV<String, String>>{
	
	@ProcessElement
	  public void processElement(ProcessContext c) {
	    String arr[]  = c.element().split(",");
	    String strKey = arr[0];
	    String strVal = arr[1]+","+arr[2]+","+arr[3];
	    c.output(KV.of(strKey, strVal));	    	
	   }
}

class LeftUserParsing extends DoFn<String,KV<String, String>>{
	
	@ProcessElement
	  public void processElement(ProcessContext c) {
	    String arr[]  = c.element().split(",");
	    String strKey = arr[0];
	    String strVal = arr[1];
	    c.output(KV.of(strKey, strVal));	    	
	   }
	
}


public class LeftJoinExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline p = Pipeline.create();

		// Step 1 : Convert String to KV object.
		
		PCollection<KV<String,String>> pOrderCollection = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section5/user_order.csv"))
								.apply(ParDo.of(new LeftOrderParsing()));
		
		PCollection<KV<String,String>> pUserCollection = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section5/p_user.csv"))
				.apply(ParDo.of(new LeftUserParsing()));
		
		

		// Step 2 create TupleTag object

		final TupleTag<String> orderTuple = new TupleTag<String>();
		final TupleTag<String> userTuple = new TupleTag<String>();
		
		//Step 3 Combine data sets using CoGroupByKey
		 
		PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple, pOrderCollection)
						.and(userTuple, pUserCollection)
						.apply(CoGroupByKey.<String>create());
		
		 
	
		// Step 4 : iterate CoGbkResult and build String  
		    
		PCollection<String> output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
				
			@ProcessElement
			  public void processElement(ProcessContext c) {
			   	    	
					String strKey = c.element().getKey();
					CoGbkResult valObject = c.element().getValue();
					
					Iterable<String> orderTable= valObject.getAll(orderTuple);
					Iterable<String> userTable = valObject.getAll(userTuple);
					
					for (String order : orderTable) {
						if(userTable.iterator().hasNext()) {
							for (String user : userTable) {							
								c.output(strKey+","+order+","+user);
							}
						}
						else {
							c.output(strKey+","+order+","+null);
						}
						
					}
			  }
		}));
		
		
		// Step 5 : save the result
		output.apply(TextIO.write().to("/home/sabb/Documents/Beam/Section5/left_join_example.csv").withNumShards(1).withSuffix(".csv"));		
		 p.run();   

	}

	

}