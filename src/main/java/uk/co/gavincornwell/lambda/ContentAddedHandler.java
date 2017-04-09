package uk.co.gavincornwell.lambda;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ContentAddedHandler implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        List<S3EventNotificationRecord> records = s3Event.getRecords();
        context.getLogger().log("There are " + records.size() + " records to handle");

        // pick out the first one, presuming there's always only one!
        S3EventNotificationRecord record = records.get(0);
        String bucketName = record.getS3().getBucket().getName();
        String objectKey = record.getS3().getObject().getKey();
        context.getLogger().log("Processing event for '" + objectKey + "' in bucket '" + bucketName + "'...");

        // retrieve metadata for the s3 object
        String id = UUID.randomUUID().toString();
        AmazonS3Client s3Client = new AmazonS3Client();
        s3Client.setRegion(Region.getRegion(Regions.EU_WEST_1));
        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, objectKey);
        String contentType = objectMetadata.getContentType();
        context.getLogger().log("contentType = " + contentType);
        long contentLength = objectMetadata.getContentLength();
        context.getLogger().log("contentLength = " + contentLength);
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        context.getLogger().log("metadata = " + userMetadata);

        // create entry in dynamo db
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
        dynamoDBClient.setRegion(Region.getRegion(Regions.EU_WEST_1));
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        Table table = dynamoDB.getTable("gavS3UploadTesting");
        context.getLogger().log("table = " + table);

        Item item = new Item()
                .withPrimaryKey("id", id)
                .withString("name", objectKey)
                .withString("contentType", contentType)
                .withLong("contentLength", contentLength);
        if (userMetadata != null) {
            for (String key : userMetadata.keySet()) {
                item.withString(key, userMetadata.get(key));
            }
        }

        context.getLogger().log("item = " + item);

        // add the item
        table.putItem(item);

        return "OK";
    }
}
