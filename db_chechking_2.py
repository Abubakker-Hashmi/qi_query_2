from numpy import dtype
import sqlalchemy
import pandas as pd
# Replace the connection details in the engine string
from sentence_transformers import SentenceTransformer
import pandas as pd
import sqlalchemy

from elasticsearch import Elasticsearch, RequestError
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-mpnet-base-v2")

engine = sqlalchemy.create_engine('mysql+pymysql://u440581820_home:&9kQ>SwFl|3X@home.propertyindex.ae/u440581820_home')
houses = pd.read_sql_table('houses',engine)
houses['price'] = houses['price'].replace('[^\d]', '', regex=True).astype(int)
print(houses)
print(houses.columns)


selected_columns = ['id', 'user_id', 'name', 'price', 'bedroom', 'bathroom', 'category', 'year', 'unit',
                    'location_name', 'long', 'lat', 'sqft', 'builtArea', 'plotArea', 'phase', 'dld', 
                    'otherdld', 'vat', 'status', 'amenitie_id', 'floor']

# Apply lambda function to selected columns and combine the result as one column
houses['vcolumn'] = houses[selected_columns].apply(lambda row: ', '.join([f"{col}: {val}" for col, val in row.items()]), axis=1)
print('after preproc')
print(houses)
print(houses.columns)
null_counts = houses.isna().sum()
print(null_counts)
blank_counts = (houses == '').sum()
print(blank_counts)
print(houses.vcolumn[0])

print('vectorizing the data...wait plz 2 minutes ')
#Vectorizing  
houses['DescriptionVector']=houses['vcolumn'].apply(lambda x: model.encode(x))
houses.head()
#print(houses['DescriptionVector'])[0]

print('entering elasticsearch domain')

es = Elasticsearch('http://13.202.16.20:9200/')
from index_mapping import index_mapping
import time


print('creating index for the first time')

index_name = "property1"

# Check if the index already exists
if not es.indices.exists(index=index_name):
    try:
        # Create the index with mappings
        es.indices.create(index=index_name, mappings=index_mapping)
        print('created index',index_name)
    except RequestError as e:
        print(f"Failed to create index: {e}")

# Now proceed with indexing the records
record_list = houses.to_dict('records')
record_list[0]


# Now proceed with indexing the records
record_list = houses.to_dict('records')
record_list[0]

for record in record_list:
        if record['id'] is not None:
            index_name = 'property1'
            document_id = record['id']  # Assuming 'id' is the unique identifier
            document =  {
                "user_id": record['user_id'],
                "name": record['name'],
                "price": record["price"],
                'bedroom': record["bedroom"],
                'bathroom': record["bathroom"],
                "category": record['category'],
                "year": record['year'],
                "unit": record['unit'],
                "location_name": record['location_name'],
                "long": record['long'],
                "lat": record['lat'],
                "sqft": record['sqft'],
                "builtArea": record['builtArea'],
                "plotArea": record['plotArea'],
                "phase": record['phase'],
                "dld": record['dld'],
                "otherdld": record['otherdld'],
                "vat": record['vat'],
                "status": record['status'],
                "amenitie_id": record['amenitie_id'],
                "floor": record['floor'],
                "DescriptionVector": record['DescriptionVector'],
            }
            if es.exists(index=index_name, id=document_id):
                
                es.update(index=index_name, id=document_id, body={"doc": document})
            else:
                
                es.index(index=index_name, id=document_id, body=document)
print('Query searching')
def search_function(query):
    #es = Elasticsearch('http://127.0.0.1:9200/')
    es = Elasticsearch('http://13.202.16.20:9200/')
    model = SentenceTransformer("all-mpnet-base-v2")

    vector_of_input_keyword = model.encode(query)

    knn_query = {
        "field": "DescriptionVector",
        "query_vector": vector_of_input_keyword,
        "k": 10,
        "num_candidates": 100,
    }

    res = es.knn_search(index="property1", knn=knn_query, source=   ['id', 'user_id', 'name', 'price', 'bedroom', 'bathroom', 'category', 'year', 'unit',
                    'location_name', 'long', 'lat', 'sqft', 'builtArea', 'plotArea', 'phase', 'dld', 
                    'otherdld', 'vat', 'status', 'amenitie_id', 'floor'])

    return res["hits"]["hits"]

query = input("Enter query to search: ")
print("Search results:")
print(search_function(query))

print("_______________________________________________")
print("ids to be picked ")
print("_______________________________________________")
id_list = [entry['_id'] for entry in search_function(query)]
#Printing the list of ids
print(id_list)
print('enring while loopfor 5 secs')


import time
start_time = time.time()


print('enring while loopfor 5 secs')
while time.time() - start_time < 5:  # 
    houses_up = houses.tail(10)
    selected_columns = ['id', 'user_id', 'name', 'price', 'bedroom', 'bathroom', 'category', 'year', 'unit',
                    'location_name', 'long', 'lat', 'sqft', 'builtArea', 'plotArea', 'phase', 'dld', 
                    'otherdld', 'vat', 'status', 'amenitie_id', 'floor']
    houses_up['vcolumn'] = houses_up[selected_columns].apply(lambda row: ', '.join([f"{col}: {val}" for col, val in row.items()]), axis=1)
    #houses_up['description'] = houses_up.apply(lambda row: ', '.join([f"{col}: {val}" for col, val in row.items()]), axis=1)
    houses_up['DescriptionVector'] = houses_up['vcolumn'].apply(lambda x: model.encode(x))
    record_list_up = houses_up.to_dict('records')

    for record in record_list_up:
        if record['id'] is not None:
            index_name = 'property1'
            document_id = record['id']  # Assuming 'id' is the unique identifier
            document = {
                "user_id": record['user_id'],
                "name": record['name'],
                "price": record["price"],
                'bedroom': record["bedroom"],
                'bathroom': record["bathroom"],
                "category": record['category'],
                "year": record['year'],
                "unit": record['unit'],
                "location_name": record['location_name'],
                "long": record['long'],
                "lat": record['lat'],
                "sqft": record['sqft'],
                "builtArea": record['builtArea'],
                "plotArea": record['plotArea'],
                "phase": record['phase'],
                "dld": record['dld'],
                "otherdld": record['otherdld'],
                "vat": record['vat'],
                "status": record['status'],
                "amenitie_id": record['amenitie_id'],
                "floor": record['floor'],
                "DescriptionVector": record['DescriptionVector'],
            }

            
            if es.exists(index=index_name, id=document_id):
                
                es.update(index=index_name, id=document_id, body={"doc": document})
            else:
                
                es.index(index=index_name, id=document_id, body=document)
    
    
    if time.time() - start_time >= 5:
        break
###testing 
print(es.count())


print(houses.tail(30))