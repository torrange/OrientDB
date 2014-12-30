#! /usr/bin/env julia

using Base, OrientDB, CSVTools

user = "tobi";
secret = "thermal2";
server = "localhost";
port = "2480";
c = OrientDB.connection_create(user, secret, server, port)
cobj = csv_read("bOnlineCRM_MongoExport.csv", ',')
features = csv_head(cobj)
data = csv_data(cobj)

for record in data
    lead_score = None
    package_price = None
    design = None
    id = None
    customer_name = None
    sold_date = None
    created_by = None
    source = None
    internal = None
    confirm_date = None
    email = None
    status = None
    added = None
    sale_type = None
    business = None
    pro_web_design_price = None
    customer_phone = None
    external = None
    upfront = None
    package = None
    industry = None
    created = None
    upfront_discount = None
    sales_person = None
    try
        lead_score = string(record[1])
    catch
        lead_score = "none";
    end


    try
        package_price = float(record[2])
    catch
        package_price = 0.0;
    end


    try
        design = string(record[3])
    catch
        design = "none";
    end


    try
        id = string(record[4])
    catch
        id = "none";
    end


    try
        customer_name = string(record[5])
    catch
        customer_name = "none";
    end


    try
        sold_date = Base.strftime(Base.strptime("%Y-%m-%d", utf8(record[6])))
    catch
        sold_date = "none";
    end


    try
        created_by = string(record[7])
    catch
        customer_name = "none";
    end


    try
        source = string(record[8])
    catch
        source = "none";
    end


    try
        internal = string(record[9])
    catch
        internal = "none";
    end


    try
        confirm_date = Base.strftime(Base.strptime("%Y-%m-%d", utf8(record[10])))
    catch
        confirm_date = "none";
    end


    try
        email = string(record[11])
    catch
        email = "none";
    end


    try
        status = string(record[12])
    catch
        status = "none";
    end


    try
        added = Base.strftime(Base.strptime("%Y-%m-%d", utf8(record[13])))
    catch
        added = "none";
    end


    try
        sale_type = string(record[14])
    catch
        sale_type = "none";
    end


    try
        business = string(record[15])
    catch
        business = "none";
    end


    try
        pro_web_design_price = float(record[16])
    catch
        pro_web_design_price = 0.0;
    end


    try
        customer_phone = string(record[17])
    catch
        customer_phone = "none";
    end


    try
        external = string(record[18])
    catch
        external = "none";
    end


    try
        upfront = float(record[19])
    catch
        upfront = 0.0;
    end


    try
        package = string(record[20])
    catch
        package = "none";
    end


    try
        industry = string(record[21])
    catch
        industry = "none";
    end


    try
        created = Base.strftime(Base.strptime("%Y-%m-%d", utf8(record[22])))
    catch
        created = "none";
    end


    try
        upfront_discount = bool_conv(record[23], "True")
    catch
        upfront_discount = false;
    end


    try
        sales_person = string(record[24])
    catch
        sales_person = "none";
    end


    rec_arr = {"@class"=>"Lead","lead_score"=>lead_score,"package_price"=>package_price,"design"=>design, "id"=>id, "customer_name"=>customer_name, "sold_date"=>sold_date, "created_by"=>created_by, "source"=>source, "internal"=>internal, "confirm_date"=>confirm_date, "email"=>email, "status"=>status, "added"=>added, "sale_type"=>sale_type, "business"=>business, "pro_web_design_price"=>pro_web_design_price, "customer_phone"=>customer_phone, "external"=>external, "upfront"=>upfront, "package"=>package, "industry"=>industry, "created"=>created, "upfront_discount"=>upfront_discount, "sales_person"=>sales_person};
    OrientDB.document_create(c, "bOnlineCRM", rec_arr)
    

end
