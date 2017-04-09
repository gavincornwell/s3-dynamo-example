package uk.co.gavincornwell.lambda;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import java.util.List;
import java.util.Map;

public class NodeAddedHandler implements RequestHandler<DynamodbEvent, String> {

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {

        List<DynamodbEvent.DynamodbStreamRecord> records = event.getRecords();
        context.getLogger().log("There are " + records.size() + " records to handle");

        for (DynamodbEvent.DynamodbStreamRecord record : records) {
            Map<String,AttributeValue> newImage = record.getDynamodb().getNewImage();
            if (newImage != null)
            {
                context.getLogger().log("A node with the following details has been " + record.getEventName() + ": " + newImage);
            }
            else
            {
                String msg = "[ERROR] The record [" + record.getEventName() + "]"
                        + record.getEventID() + " will be skipped as there is no image";
                context.getLogger().log(msg);
            }
        }

        return "OK";
    }
}
