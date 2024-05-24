from flask import Flask, request, jsonify
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
import time
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

es = Elasticsearch('http://13.202.16.20:9200/')
model = SentenceTransformer("all-mpnet-base-v2")

app = Flask(__name__)

# Sample relevant property descriptions (these should be replaced with actual relevant descriptions)
sample_descriptions = [
    "3 bedroom apartment in downtown",
    "luxury villa with 4 bathrooms and a pool",
    "spacious 2 bedroom condo near the beach",
    '2 bedrooms','2 bd',' 2 bhk','buidings in dubai'
    # Add more relevant descriptions as needed
]

# Calculate the average vector for the sample descriptions
sample_vectors = model.encode(sample_descriptions)
average_vector = np.mean(sample_vectors, axis=0)

def search_function(query):
    vector_of_input_keyword = model.encode(query)
    
    # Calculate cosine similarity with the average vector
    similarity = cosine_similarity([vector_of_input_keyword], [average_vector])[0][0]
    
    # Set a similarity threshold
    similarity_threshold = 0.3
    
    if similarity < similarity_threshold:
        return {"error": "Please provide a more relevant query."}
    
    knn_query = {
        "field": "DescriptionVector",
        "query_vector": vector_of_input_keyword,
        "k": 10,
        "num_candidates": 100,
    }

    res = es.knn_search(index="property1", knn=knn_query, source=[
        'id', 'user_id', 'name', 'price', 'bedroom', 'bathroom', 'category', 'year', 'unit',
        'location_name', 'long', 'lat', 'sqft', 'builtArea', 'plotArea', 'phase', 'dld', 
        'otherdld', 'vat', 'status', 'amenitie_id', 'floor'
    ])
    return res["hits"]["hits"]

@app.route('/')
def index():
    return jsonify({"message": "Welcome to the property search API. Use POST /search with a JSON payload containing a 'query' field."})

@app.route('/search', methods=['GET'])
def search():
    if request.method == 'GET':
        data = request.get_data(as_text=True)
        if not data:
            return jsonify({"error": "Invalid request. Provide a non-empty query."}), 400
        
        query = data.strip()
        start_time = time.time()
        search_results = search_function(query)
        
        # If the search_function returns an error, handle it
        if isinstance(search_results, dict) and "error" in search_results:
            return jsonify(search_results), 400
        
        end_time = time.time()
        
        results = []
        for result in search_results:
            source = result["_source"]
            results.append({
                "ID": result["_id"],
                "User ID": source.get("user_id"),
                "Name": source.get("name"),
                "Price": source.get("price"),
                "Bedroom": source.get("bedroom"),
                "Bathroom": source.get("bathroom"),
                "Category": source.get("category"),
                "Year": source.get("year"),
                "Unit": source.get("unit"),
                "Location": source.get("location_name"),
                "Longitude": source.get("long"),
                "Latitude": source.get("lat"),
                "Sqft": source.get("sqft"),
                "Built Area": source.get("builtArea"),
                "Plot Area": source.get("plotArea"),
                "Phase": source.get("phase"),
                "DLD": source.get("dld"),
                "Other DLD": source.get("otherdld"),
                "VAT": source.get("vat"),
                "Status": source.get("status"),
                "Amenitie ID": source.get("amenitie_id"),
                "Floor": source.get("floor")
            })
        
        return jsonify({
            "query": query,
            "execution_time": end_time - start_time,
            "results": results
        })



'''
from flask import Flask, request, jsonify, render_template
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
#from db_chechking_2 import search_function
import time

es = Elasticsearch('http://65.2.150.189:9200/')
model = SentenceTransformer("all-mpnet-base-v2")

app = Flask(__name__)

def search_function(query):
    vector_of_input_keyword = model.encode(query)
    knn_query = {
        "field": "DescriptionVector",
        "query_vector": vector_of_input_keyword,
        "k": 10,
        "num_candidates": 100,
    }

    res = es.knn_search(index="property1", knn=knn_query, source=['id', 'user_id', 'name', 'price', 'bedroom', 'bathroom', 'category', 'year', 'unit',
                    'location_name', 'long', 'lat', 'sqft', 'builtArea', 'plotArea', 'phase', 'dld', 
                    'otherdld', 'vat', 'status', 'amenitie_id', 'floor'])
    return res["hits"]["hits"]

@app.route('/')
def index():
    return render_template('index.html')
@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        query = request.form.get('query')
        if query:
            start_time = time.time()
            search_results = search_function(query)
            end_time = time.time()
            return render_template('search_results.html', query=query, search_results=search_results, execution_time=end_time-start_time)
    return render_template('search.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
'''