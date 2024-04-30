import ballerina/http;
import ballerina/log;
import ballerina/mime;
import ballerina/os;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/aws.s3;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

listener http:Listener httpListener = new (8080);

string AWSACCESSKEY = os:getEnv("AWS_ACCESS_KEY");
string AWSSECRETKEY = os:getEnv("AWS_SECRET_KEY");

string MYSQL_HOST = os:getEnv("MYSQL_HOST");
string MYSQL_USER = os:getEnv("MYSQL_USER");
string MYSQL_PASSWORD = os:getEnv("MYSQL_PASSWORD");
string MYSQL_DB = os:getEnv("MYSQL_DB");
int MYSQL_PORT = 27207;

s3:ConnectionConfig awsS3Config = {
    accessKeyId: AWSACCESSKEY,
    secretAccessKey: AWSSECRETKEY,
    region: "eu-west-2"
};

s3:Client awsS3Client = check new (awsS3Config);

mysql:Client|sql:Error dbClient = new (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_PORT);

type Data record {|
    int Id;
    string Description;
|};

service / on httpListener {

    resource function get show() returns string {
        return "hello";
    }

    resource function post store(http:Request request) returns http:Response|http:InternalServerError|error {
        var multipartData = request.getBodyParts();

        if (multipartData is http:ClientError) {
            var errorMessage = multipartData.message();
            log:printError("Error retrieving body parts: " + errorMessage);
            log:printError("Detailed error: " + multipartData.detail().toString());
            return error http:Error("Internal Server Error");
        }

        http:Response response = new;

        foreach var part in multipartData {
            var filename = self.handleContent(part);

            if (filename is string) {
                // Add the filename to mysql table together with User Id
                _ = check self.AddDataItem(filename);

                response.setPayload("File uploaded successfully");
                return response;
            }
            else {
                return error http:Error("Internal Server Error");
            }
        }

        response.setPayload("File uploaded successfully");
        return response;
    }

    function handleContent(mime:Entity bodyPart) returns string|error {
        var mediaType = mime:getMediaType(bodyPart.getContentType());
        if mediaType is mime:MediaType {

            // check if subtype is image
            if (mediaType.primaryType == "image") {

                var byteStream = bodyPart.getByteArray();
                if byteStream is byte[] {
                    string uuid1 = uuid:createType1AsString();
                    var filename = uuid1 + "." + mediaType.subType;
                    var createObject = awsS3Client->createObject("foodbuddy", filename, byteStream);
                    if (createObject is error) {
                        log:printError("Error creating object in S3: " + createObject.message());
                        return error("Internal Server Error");
                    }
                    else {
                        return filename;
                    }

                } else {
                    log:printError("Error parsing byte array: " + byteStream.message());
                    return error("Internal Server Error");
                }

            }
        }
        return error("Internal Server Error");
    }

    # GetDataItemById - This method is used to get an item from the databae
    #
    # + id - Id of the data item to retrieve
    # + return - Ruturn the added data item if passed, or return error if something failed. 
    public function GetDataItemById(int id) returns Data|error {

        mysql:Client mysqlClient = check new (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_PORT);

        sql:ParameterizedQuery GetDataItemByIdQuery = `SELECT * FROM recipes WHERE Id = ${id}`;
        stream<record {}, sql:Error?> resultStream = mysqlClient->query(GetDataItemByIdQuery);

        record {|
            record {} value;
        |}|error? result = resultStream.next();

        if (result is record {|
            record {} value;
        |}) {
            //Map result into structure
            Data addedItem = {
                Id: <int>result.value["Id"],
                Description: <string>result.value["Description"]
            };
            return addedItem;

        } else if (result is error) {
            log:printError("Next operation on the stream failed!:" + result.message());
            return error(result.message());
        } else {
            return error("Retreive failed");
        }
    }

    # AddDataItem - This method is used to add an item to the databae
    #
    # + entry - Entry Description of the data item
    # + return - Ruturn the added data item if passed, or return error if something failed.  
    public function AddDataItem(string entry) returns Data|error {

        log:printInfo("SQL AddDataItem Method Reached");
        mysql:Client mysqlClient = check new (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_PORT);
        sql:ParameterizedQuery InsertNewDataItemQuery = `INSERT INTO recipes (user_id, image) VALUES(${entry}, ${entry})`;

        sql:ExecutionResult|sql:Error queryResult = mysqlClient->execute(InsertNewDataItemQuery);

        if (queryResult is sql:ExecutionResult) {
            log:printInfo("Insert success");

            //Retrieve the inserted value
            return self.GetDataItemById(<int>queryResult.lastInsertId);

        } else {
            log:printError("Error occurred");
            return queryResult;
        }

    }

}

