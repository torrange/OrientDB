module CSVTools

export csv_read,
       csv_head,
       csv_tail,
       csv_data,
       csv_col_indexes,
       bool_conv


function csv_read(fname, dlim)
    csv_array = readdlm(fname)
    csv_object = {"array"=>csv_array, "dlim"=>dlim}
    return csv_object
end


function csv_head(csv_object)
    head = split(csv_object["array"][1], csv_object["dlim"])
    return head
end


function csv_col_indexes(row)
    ccount = 1
    for cell in row
        #col_map[cell] = ccount
        print(string(ccount, ": ", cell, "\n"))
        ccount += 1
    end
end


function csv_tail(csv_object)
    tail = split(csv_object["array"][length(csv_object["array"])],  csv_object["dlim"])
    return tail
end


function csv_data(csv_object)
    data = [split(row, csv_object["dlim"]) for row in csv_object["array"][2:length(csv_object["array"])]]
    return data
end


function bool_conv(boolstring, true_string)
    if boolstring == true_string
        bool_type = true
    else
        bool_type = false
    end
    return bool_type
end


end
