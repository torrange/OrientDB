module OrientDB
using JSON, Requests


function create_connection(user, secret, server, port)
  connection_url = string("http://", user, ":", secret, "@", server,":", port)
  return connection_url
end


function get_database(connection_url, dbname)
  request = get(string(connection_url, "/database/", dbname))
  decoded = JSON.parse(request.data)
  return decoded
end


function connect_database(connection_url, dbname)
  request = get(string(connection_url, "/connect/", dbname))
  response = { "status" => None }
  if request.status == 204
    response["status"] = "connected"
  else
    response["status"] = "failed"
  end
  return response
end


function document_create(connection_url, dbname, document)
  request = post(string(connection_url, "/document/", dbname), json=document)
  decoded = JSON.parse(request.data)
  return decoded
end


function document_get(connection_url, dbname, record_id)
  request = get(string(connection_url, "/document/", dbname, "/", record_id))
  decoded = JSON.parse(request.data)
  return decoded
end


function document_check(connection_url, dbname, record_id)
  request = head(string(connection_url, "/document/", dbname, "/", record_id))
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


function class_property_create(connection_url, dbname, class_name, property_name, property_type="String")
  url = string(connection_url, "/property/", dbname, "/", class_name, "/", property_name,"/", property_type)
  request = post(url)
  response = { "status" => None }
  if request.status == 201
    response["status"] = "created"
  else
    response["status"] = "failed"
  end
  return request, response
end


end
