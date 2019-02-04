package pt.intesctec.nlp.rapidminer.operators;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

public class YakeOperator extends Operator {

	private OutputPort exampleSetOutput = getOutputPorts().createPort("keywords");
	private InputPort exampleSetInput = getInputPorts().createPort("keywords");
	
	public static final String PARAMETER_LANGUAGE = "language";
	public static final String PARAMETER_NGRAM_SIZE = "ngram size";
	public static final String PARAMETER_NUMBER_OF_KEYWORDS = "number of keywords";
	//public static final String PARAMETER_WINDOW_SIZE = "window size";
	
	public YakeOperator(OperatorDescription description) {
		super(description);
		// TODO Auto-generated constructor stub
	     getTransformer().addGenerationRule(exampleSetOutput, ExampleSet.class);
	}

	@Override
	public List<ParameterType> getParameterTypes(){
		List<ParameterType> types = super.getParameterTypes();

	    types.add(new ParameterTypeString(
	    		PARAMETER_LANGUAGE,
	        "This parameter defines the expected language from the input text",
	        "en",
	        true));
	    
	    types.add(new ParameterTypeString(
	    		PARAMETER_NGRAM_SIZE,
	        "This parameter defines the maximum size of an ngram candidate",
	        "3",
	        true));
	    
	    types.add(new ParameterTypeString(
	    		PARAMETER_NUMBER_OF_KEYWORDS,
	        "This parameter defines the maximum number of keyword candidates to return",
	        "10",
	        true));
	    
	    
	    return types;
	}
	

    @Override
    public void doWork() throws OperatorException {
    	
    	List<JSONObject> keywords = this.execRemoteYake("Extracting keywords from texts has become a challenge for individuals and organizations as the information grows in complexity and size. The need to automate this task so that texts can be processed in a timely and adequate manner has led to the emergence of automatic keyword extraction tools. Despite the advances, there is a clear lack of multilingual online tools to automatically extract keywords from single documents. In this paper, we present Yake!, a novel feature-based system for multi-lingual keyword extraction, which supports texts of different sizes, domain or languages. Unlike most of the systems, Yake! does not rely on dictionaries nor thesauri, neither is trained against any corpora. Instead, we follow an unsupervised approach which builds upon features extracted from the text, making it thus applicable to documents written in different languages without the need for further knowledge. This can be beneficial for a large number of tasks and a plethora of situations where the access to training corpora is either limited or restricted. In this demo, we offer an easy to use, interactive session, where users from both academia and industry can try our system, either by using a sample document or by introducing their own text. As an add-on, we compare our extracted keywords against the output produced by the IBM Natural Language Understanding and Rake system. This will enable users to understand the distinctions between the three approaches.");
		
        // create the needed attributes
        Attribute[] attributes = new Attribute[2];
        ExampleSet exampleSet;

        Attribute newStringAttribute = AttributeFactory.createAttribute("ngram",Ontology.ATTRIBUTE_VALUE_TYPE.STRING);
        Attribute newRealNumberAttribute = AttributeFactory.createAttribute("score",Ontology.ATTRIBUTE_VALUE_TYPE.REAL);
        
        attributes[0] = newStringAttribute;
        attributes[1] = newRealNumberAttribute;

        // basis is a MemoryExampleTable, so create one and pass it the 
        // list of attributes it should contain
        MemoryExampleTable table = new MemoryExampleTable(attributes);
        DataRowFactory rowFactory = new DataRowFactory(0);
        
        String[] strings= new String[2];
        
        for (int i = 0; i < keywords.size(); i++) {
        	  // make and add row
        	JSONObject kw = keywords.get(i);
        	
        	try {
			
        		strings[0] = kw.get("ngram").toString();
				strings[1] = kw.get("score").toString();

        	} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            DataRow row = rowFactory.create(strings, attributes); 
            table.addDataRow(row);	
            
        }

        // finally create the ExampleSet from the table
        exampleSet = table.createExampleSet();

        exampleSetOutput.deliver(exampleSet);
    }
    
    //public static List<JSONObject> execRemoteYake(String content, int numberOfKeywords, int maxNgramSize) {
    public List<JSONObject> execRemoteYake(String content) {
    //curl -X POST "http://yake.inesctec.pt/yake/v2/extract_keywords
    	//?max_ngram_size=4&number_of_keywords=20" -H "accept: application/json" 
    	//-H "Content-Type: application/x-www-form-urlencoded" -d "content=f%20kjsdfgdfjkl%20sdklfjg%20sldkfj%20gsd%20fg"
        
    	String language = "en";
    	String number_of_keywords = "10";
    	String max_ngram_size = "3";
    	
    	try {
			language = getParameterAsString(PARAMETER_LANGUAGE);
			max_ngram_size = getParameterAsString(PARAMETER_NGRAM_SIZE);
			number_of_keywords = getParameterAsString(PARAMETER_NUMBER_OF_KEYWORDS);
		} catch (UndefinedParameterError e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	String yake_api_remote_url = "http://yake.inesctec.pt/yake/v2/extract_keywords?number_of_keywords="+number_of_keywords+"&max_ngram_size=" + max_ngram_size ;
    	HashMap<String,String> parameterList = new HashMap<String,String>();
    	parameterList.put("content", content);
    	
    	String result = postRequest(yake_api_remote_url, parameterList);
    	System.out.print(result);
    	
    	List<JSONObject> result_keywords = new ArrayList<JSONObject>();
    	
    	try {
			JSONObject jsonObj = new JSONObject(result);
			if(jsonObj.has("keywords")) {
				JSONArray keywords = jsonObj.getJSONArray("keywords");
				for (int i = 0; i < keywords .length(); i++) {
					JSONObject kw = (JSONObject) keywords.get(i);
					System.out.println(kw);
					result_keywords.add(kw);
				}	
			}
			
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    	return result_keywords;
        
        //("number_of_keywords", "20");
        //("max_ngram_size", "3");
        //("content", content);
        
    
		/*
		 * LogService.getRoot().log(Level.INFO,response.toString()); return
		 * response.toString();
		 * 
		 * } else {
		 * 
		 * LogService.getRoot().log(Level.WARNING,"GET NOT WORKED"); return "error"; }
		 */
    }

	public  static String postRequest( String mainUrl, HashMap<String,String> parameterList)
	{
	   String response="";
	   try {
	       URL url = new URL(mainUrl);
	
	       StringBuilder postData = new StringBuilder();
	       for (Map.Entry<String, String> param : parameterList.entrySet())
	       {
	           if (postData.length() != 0) postData.append('&');
	           postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
	           postData.append('=');
	           postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
	       }
	
	       byte[] postDataBytes = postData.toString().getBytes("UTF-8");
	
	
	
	
	       HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	       conn.setRequestMethod("POST");
	       conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
	       conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
	       conn.setDoOutput(true);
	       conn.getOutputStream().write(postDataBytes);
	
	       Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
	
	       StringBuilder sb = new StringBuilder();
	       for (int c; (c = in.read()) >= 0; )
	           sb.append((char) c);
	       response = sb.toString();
	
	
	   return  response;
	   }catch (Exception excep){
	       excep.printStackTrace();}
	   return response;
	}
    
  
}
