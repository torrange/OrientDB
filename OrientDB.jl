module OrientDB
using JSON, Requests


function connection_create(user, secret, server, port)
  connection_url = string("http://", user, ":", secret, "@", server,":", port)
  return connection_url
end


function connection_command(connection_url, command, id)
  url = string(connection_url, "/connection/", command, "/", id)
  request = post(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function server_get(connection_url)
  url = string(connection_url, "/server")
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function server_disconnect(connection_url)
  url = string(connection_url, "/connect/", dbname)
  request = get(url)
  response = { "status" => None }
  if request.data == None
    response["status"] = "disconnected"
  else
    response["status"] = "failed"
  end
  return response
end


function databases_list(connection_url)
  url = string(connection_url, "/listDatabases")
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function database_get(connection_url, dbname)
  url = string(connection_url, "/database/", dbname)
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function database_create(connection_url, dbname, dbtype)
  url = string(connection_url, "/database/", dbname, "/", dbtype)
  request = post(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function database_delete(connection_url, dbname)
  url = string(connection_url, "/database/", dbname)
  request = delete(url)
  response = { "status" => None }
  if request.status == 204
    response["status"] = "deleted"
  else
    response["status"] = "failed"
  end
  response["request"] = request
  return response
end


function database_connect(connection_url, dbname)
  url = string(connection_url, "/connect/", dbname)
  request = get(url)
  response = { "status" => None }
  if request.status == 204
    response["status"] = "connected"
  else
    response["status"] = "failed"
  end
  return response
end


function document_create(connection_url, dbname, document)
  url = string(connection_url, "/document/", dbname)
  request = post(url, json=document)
  decoded = JSON.parse(request.data)
  return decoded
end


function document_get(connection_url, dbname, record_id)
  url = string(connection_url, "/document/", dbname, "/", record_id)
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function document_check(connection_url, dbname, record_id)
  url = string(connection_url, "/document/", dbname, "/", record_id)
  request = head(url)
  response = { "status" => None }
  if request.status == 204
    response["status"] = "success"
  else
    response["status"] = "failed"
  end
  return response
end


function document_update(connection_url, dbname, document, record_id)
  url = string(connection_url, "/document/", dbname, "/", record_id)
  url = string(url, "?updateMode=full")
  request = put(url, json=document)
  return request
end


function class_get(connection_url, dbname, class_name)
  url = string(connection_url, "/class/", dbname, "/", class_name)
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function class_create(connection_url, dbname, class_name)
  url = string(connection_url, "/class/", dbname, "/", class_name)
  request = post(url)
  response = { "status" => None }
  if request.status == 201
    response["status"] = "created"
  else
    response["status"] = "failed"
  end
  return response
end


function class_property_create(connection_url, dbname, class_name, property_name, property_type=None)
  if property_type == None
    property_type = ""
  end
  url = string(connection_url, "/property/", dbname, "/", class_name, "/", property_name, "/", property_type)
  request = post(url)
  response = { "status" => None }
  if request.status == 201
    response["status"] = "created"
  else
    response["status"] = "failed"
  end
  response["request"] = request
  return response
end


function class_property_create_multiple(connection_url, dbname, class_name, properties)
  url = string(connection_url, "/property/", dbname, "/", class_name, "/")
  request = post(url, json=properties)
  response = { "status" => None }
  if request.status == 201
    response["status"] = "created"
  else
    response["status"] = "failed"
  end
  response["request"] = request
  return response
end


function cluster_get(connection_url, dbname, cluster_name)
  url = string(connection_url, "/cluster/", dbname, "/", cluster_name)
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function command(connection_url, dbname, language, command_text, limit=20)
  url = string(connection_url, "/command/", dbname, "/", language, "/", command_text, "/", limit)
  url = replace(url, " ", "%20")
  request = post(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function query(connection_url, dbname, language, query_text, limit=20)
  url = string(connection_url, "/query/", dbname, "/", language, "/", query_text, "/", limit)
  url = replace(url, " ", "%20")
  request = get(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function batch(connection_url, dbname, batch_job)
  url = string(connection_url, "/batch/", dbname)
  request = post(url, json=batch_job)
  decoded = JSON.parse(request.data)
  return decoded
end


function function_execute(connection_url, dbname, function_name, arguments)
  url = string(connection_url, "/function/", dbname, "/", function_name)
  if length(arguments) > 0
    for argument in arguments
      url = string(url, "/", argument)
    end
  end
  request = post(url)
  decoded = JSON.parse(request.data)
  return decoded
end


function export_database(connection_url, dbname)
  url = string(connection_url, "/export/", dbname, "/")
  fname = string(dbname, ".gzip")
  fw_io = open(fname, "w")
  response = {"status" => None}
  try
    write(fw_io, r.data)
    close(fw_io)
    response = {"status" => "success", "filename" => fname, "database" => dbname}
  catch
    response = {"status" => "failed","database" => dbname}
  end
  return response
end


end
