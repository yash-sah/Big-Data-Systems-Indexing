import re
from flask import Flask, request, jsonify, make_response
import redis
import hashlib

app = Flask(__name__)
redis_db = redis.Redis()

# Function to validate the schema
def validate_schema(schema):
    if not isinstance(schema, dict):
        return False, "Invalid schema. The root object should be a dictionary."

    if 'planCostShares' not in schema or not isinstance(schema['planCostShares'], dict):
        return False, "Invalid schema. 'planCostShares' should be a dictionary."

    plan_cost_shares = schema['planCostShares']
    if 'copay' in plan_cost_shares and not isinstance(plan_cost_shares['copay'], int):
        return False, "Invalid schema. 'copay' in 'planCostShares' should be an integer."

    if 'deductible' in plan_cost_shares and not isinstance(plan_cost_shares['deductible'], int):
        return False, "Invalid schema. 'deductible' in 'planCostShares' should be an integer."

    if 'linkedPlanServices' in schema and isinstance(schema['linkedPlanServices'], list):
        linked_services = schema['linkedPlanServices']
        for i, linked_service in enumerate(linked_services, start=1):
            if 'linkedService' in linked_service and isinstance(linked_service['linkedService'], dict):
                linked_service_data = linked_service['linkedService']
                if 'name' in linked_service_data and not isinstance(linked_service_data['name'], str):
                    return False, f"Invalid schema. 'name' in 'linkedService' of element {i} should be a string."

            if 'planserviceCostShares' in linked_service and isinstance(linked_service['planserviceCostShares'], dict):
                planservice_cost_shares = linked_service['planserviceCostShares']
                if 'copay' in planservice_cost_shares and not isinstance(planservice_cost_shares['copay'], int):
                    return False, f"Invalid schema. 'copay' in 'planserviceCostShares' of element {i} should be an integer."
                if 'deductible' in planservice_cost_shares and not isinstance(planservice_cost_shares['deductible'], int):
                    return False, f"Invalid schema. 'deductible' in 'planserviceCostShares' of element {i} should be an integer."

    if 'planType' in schema and not isinstance(schema['planType'], str):
        return False, "Invalid schema. 'planType' should be a string."

    if 'creationDate' in schema and not re.match(r'\d{2}-\d{2}-\d{4}', schema['creationDate']):
        return False, "Invalid schema. 'creationDate' should be in the format 'mm-dd-yyyy'."

    return True, "Schema is valid."


# GET operation to retrieve the JSON schema
@app.route('/schema/<object_id>', methods=['GET'])
def get_schema(object_id):
    redis_key = f'json_schema:{object_id}'
    schema = redis_db.get(redis_key)
    if schema:
        schema = eval(schema.decode('utf-8'))
        if schema['objectId'] == object_id:
            schema_str = str(schema)
            schema_hash = hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
            if request.headers.get('If-None-Match') == schema_hash:
                return make_response('', 304, {'ETag': schema_hash})

            response = jsonify({'schema': schema})
            response.headers['ETag'] = schema_hash
            return response
    return jsonify({'message': 'Schema not found'}), 404


# POST operation to save the JSON schema
@app.route('/schema', methods=['POST'])
def save_schema():
    schema = request.get_json()
    is_valid, error_message = validate_schema(schema)
    if not is_valid:
        return jsonify({'message': error_message}), 400

    schema_str = str(schema)
    schema_hash = hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
#   redis_db.set('json_schema', schema)
    object_id = schema.get('objectId', '')
    redis_key = f'json_schema:{object_id}'
    redis_db.set(redis_key, schema_str)
    response = jsonify({'message': 'Schema created','objectId': object_id})
    response.headers['ETag'] = schema_hash
    return response, 201

# DELETE operation to delete the JSON schema
@app.route('/schema/<object_id>', methods=['DELETE'])
def delete_schema(object_id):
    redis_key = f'json_schema:{object_id}'
    schema = redis_db.get(redis_key)
    if schema:
        schema = eval(schema.decode('utf-8'))
        if schema['objectId'] == object_id:
            redis_db.delete(redis_key)
            return jsonify({'message': 'Schema deleted'}), 204
    return jsonify({'message': 'Schema not found'}), 404


if __name__ == '__main__':
    app.run(debug=True)
