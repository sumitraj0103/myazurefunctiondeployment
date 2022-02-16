import logging
import azure.functions as func
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import json
import sqlparse
from sql_metadata import Parser


cosmos_default_cols = ['_rid','_self','_etag','_attachments','_ts']
response_dict = {
    "200": "ok",
    "400": "Bad Request",
    "401": "Unauthorized",
    "403": "Forbidden",
    "404": "Not Found",
    "429": "Too Many Request",
    "500": "Internal Server Error",
    "503": "Server Unavailable",
    "502": "Bad Gateway",
    "504": "Gateway Timeout"
}

def query_cosmos(container_client, final_select_cols, final_where_cond):
    if final_where_cond:
        query = f"select {final_select_cols} from c where {final_where_cond}"
    else:
        query = f"select {final_select_cols} from c"
    items = container_client.query_items(query, enable_cross_partition_query = True)
    results = list(items)
    return results

def extract_col_list(container_client):
  first_row_item = container_client.query_items("select * from c offset 0 limit 1", 
                                                enable_cross_partition_query = True)
  first_row_dict = dict(list(first_row_item)[0])
  cols_list = list(first_row_dict.keys())
  for default_col in cosmos_default_cols:
    cols_list.remove(default_col)
  return cols_list

def filter_cols_preprocess(cols):
    col_list = []
    for col in cols:
        col_list.append("c.{}".format(col))
    final_cols = ",".join(col_list)
    return final_cols

# def where_cond_preprocess_add_container(where_cond, cols_list):
#     if where_cond:
#         for col in cols_list:
#           where_cond = where_cond.replace(col, "c.{}".format(col))
#     else:
#         where_cond = ""
#     return where_cond

def filter_result_to_list(result_list, select_cols):
    filtered_result_list = []
    for item in list(result_list):
        result_dict = {}
        for col in select_cols:
            result_dict[col.lower()] = item[col]
        filtered_result_list.append(result_dict)
    return filtered_result_list

def generate_output_json(result_list):
    base_json_str = json.dumps({}, indent = 4)
    base_json = json.loads(base_json_str)
#     base_json["statusHTTP"] = {"code": 200, "message": "ok"}
    if len(result_list) == 0:
      base_json["status"] = {"code": 1001, "message": "Record not found"}
    else:
      base_json["status"] = {"code": 1000, "message": "ok"}
    base_json["data"] = result_list
    result_json = json.dumps(base_json, indent = 4)
    return result_json


def where_cond_preprocess_add_container(where_cond, cols_list):
    # this is an interim solution
    # future solution: extract column names from where cond using regex
    tokens = where_cond.split(" ")
    new_token_list = []
    for idx, token in enumerate(tokens):
      if token in cols_list:
        token = token.upper()
        tokens[idx] = f"c.{token}"
    new_where_cond = " ".join(tokens)
    return new_where_cond

def get_where_cond_cols(where_cond):
  # create a temp_stmt that can be used with Parser
  temp_stmt = f"select * from temp_tbl_nm where {where_cond}"
  where_cond_cols_list = Parser(temp_stmt).columns
  where_cond_cols_list.remove("*")
  return where_cond_cols_list

def exit_prog_fail(e, return_code):
    # result_dict = {"statusCode": return_code, "message": response_dict[str(return_code)], 
    # "Error Message": e}
    # result_json = json.dumps(result_dict, indent=4)
    base_json_str = json.dumps({}, indent = 4)
    base_json = json.loads(base_json_str)
    base_json["statusHTTP"] = {"code": 200, "message": "ok"}  # indicating that HTTP request was a success
    base_json["statusAPI"] = {"code": return_code, "message": f"{e}"}
    result_json = json.dumps(base_json, indent = 4)
    return func.HttpResponse(result_json,
             status_code = 200
        ) 

def exit_prog_success(result_json, return_code):
    return func.HttpResponse(result_json,
        status_code = return_code
    )

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    # Settings used to initialize the Cosmos client
    # May be changed to Keyvault's secret 
    endpoint = "https://datax-dev-cosmosdb.documents.azure.com:443/"
    key = 'AY8AGZLG9ybT2gQH5PvgE5NNr9KGiOBR7Sy6CSDtzyZ45W1DvXXQi9X2f7lESfoqj41BPykWcYQs9igPgVID7Q=='

    # database and table
    database = 'api_test_db'
    headersAsDict = dict(req.headers)    
    container = headersAsDict["container"]
    if not database or not container:
        error_msg = "Cannot read container name from the header"
        return exit_prog_fail(error_msg, 9000)

    # get parameters from the Request's Body
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        cols = req_body.get('columns')
        cols = [col.upper() for col in cols]  # standard: column names are uppercase
        where_cond = req_body.get('filterCondition')
    if not cols:
        error_msg = "Cannot read column list from the request body"
        return exit_prog_fail(error_msg, 9001)
    if not where_cond:
        error_msg = "Cannot read where condition from the request body"
        return exit_prog_fail(error_msg, 9002)
    
    # get container client, to be used for query
    try:
        client = CosmosClient(endpoint, key)
        db_client = client.get_database_client(database)
        container_client = db_client.get_container_client(container)
    except Exception as e:
        error_msg = "Getting Cosmos DB client returned with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9003)

    try:
        where_cond_cols_list = get_where_cond_cols(where_cond)
    except Exception as e:
        error_msg = "Getting the where condition's columns returned with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9004)

    try:
        select_cols_list = list(set().union(cols, where_cond_cols_list))
    except Exception as e:
        error_msg = "Getting the list of columns to be selected returned with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9005)

    # preprocessing to add prefiex 'c.<column_name>' to every cols in where_cond
    try:
        final_where_cond = where_cond_preprocess_add_container(where_cond, select_cols_list)
    except Exception as e:
        error_msg = "Where condition pre-processing failed with error: {}".format(e)
        return exit_prog_fail(error_msg, 9006)
    
    # preprocessing to add prefiex 'c.<column_name>' to every cols in select
    try:
        final_select_cols = filter_cols_preprocess(select_cols_list)
    except Exception as e:
        error_msg = "Select columns pre-processing failed with error: {}".format(e)
        return exit_prog_fail(error_msg, 9007)
    
    # query from cosmos client
    try:
        result_list = query_cosmos(container_client, final_select_cols, final_where_cond)
    except Exception as e:
        error_msg = "Query to Cosmos DB failed with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9008)
    
    # filter result dict according to select columns
    try:
        filtered_result_list = filter_result_to_list(result_list, cols)
    except Exception as e:
        error_msg = "Filtering result's columns failed with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9009)
    
    # check if the number of rows in the result list exceeds the limit (current: 1000)
    if len(filtered_result_list) >= 1000:
        error_msg = "Result exceeds the limit of 1000 records"
        return exit_prog_fail(error_msg, 9010)

    # generate the result in the json format
    try:
        result_json = generate_output_json(filtered_result_list)
    except Exception as e:
        error_msg = "Generating Json result failed with an error: {}".format(e)
        return exit_prog_fail(error_msg, 9011)

    # return the results with success
    return exit_prog_success(result_json, 200)
