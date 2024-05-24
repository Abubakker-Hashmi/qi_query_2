index_mapping = {
    "properties": {

      "id": { "type": "long" },
      "price": { "type": "long" },
      "bedroom": { "type": "long" },
      "bathroom": { "type": "long" },
      "sqft": { "type": "long" },
      "builtArea": { "type": "long" },
      "location_name": { "type": "text" },
      "description": { "type": "text" },
      "DescriptionVector": {
        "type": "dense_vector",
        "dims": 768,
        "index": True,
        "similarity": "l2_norm"
      }
    }
  }
