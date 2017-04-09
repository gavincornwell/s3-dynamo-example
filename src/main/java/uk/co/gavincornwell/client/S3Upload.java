package uk.co.gavincornwell.client;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class S3Upload {

    private static final String BUCKET_NAME = "gav-s3upload-testing";

    public static void main(String[] args) throws Exception {

        System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setRegion(Region.getRegion(Regions.EU_WEST_1));

        // check bucket exists and how many things are in it
        Bucket uploadBucket = null;
        List<Bucket> buckets = s3.listBuckets();
        System.out.println("There are " + buckets.size() + " buckets in total");
        for (Bucket bucket : s3.listBuckets()) {
            if (BUCKET_NAME.equals(bucket.getName())) {
                uploadBucket = bucket;
            }
        }

        if (uploadBucket == null) {
            throw new RuntimeException(BUCKET_NAME + " does not exist, please create and re-run example");
        }

        System.out.println("Bucket owner: " + uploadBucket.getOwner().getDisplayName());
        System.out.println("Bucket created at: " + uploadBucket.getCreationDate().toString());
        System.out.println("Bucket location: " + s3.getBucketLocation(BUCKET_NAME));

        // upload a simple text file
        System.out.println("Uploading a new file...");
        String fileName = "text-" + System.currentTimeMillis() + ".txt";
        s3.putObject(new PutObjectRequest(BUCKET_NAME, fileName, createSampleFile()));
        System.out.println("Uploaded file");

        // download the simple text file
        System.out.println("Downloading the new file...");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, fileName));
        Map<String, Object> metadata = object.getObjectMetadata().getRawMetadata();
        System.out.println("metadata: " + metadata);
        System.out.println("File contents:");
        displayTextInputStream(object.getObjectContent());

        // delete the simple text file
//        System.out.println("Deleting file...");
//        s3.deleteObject(BUCKET_NAME, fileName);
//        System.out.println("Deleted file");

        // upload a file with some metadata (to be used by the triggered lambda)
        System.out.println("Uploading a new file with metadata...");
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata("title", "Test File");
        objectMetadata.addUserMetadata("description", "File used for testing upload to S3");
        fileName = "text-" + System.currentTimeMillis() + ".txt";
        File tempFile = createSampleFile();
        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, fileName,
                new FileInputStream(tempFile), objectMetadata);
        s3.putObject(putObjectRequest);
        System.out.println("Uploaded file");

        // delete the simple text file
//        System.out.println("Deleting file with metadata...");
//        s3.deleteObject(BUCKET_NAME, fileName);
//        System.out.println("Deleted file with metadata");

        // list upload operations in progress
        ListMultipartUploadsRequest allMultpartUploadsRequest = new ListMultipartUploadsRequest(BUCKET_NAME);
        MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(allMultpartUploadsRequest);
        List<MultipartUpload> uploads = multipartUploadListing.getMultipartUploads();
        System.out.println("There are " + uploads.size() + " uploads in progress");

        /*
        // upload a large file using transfer acceleration and multi part
        // Create a list of UploadPartResponse objects. You get one of these for
        // each part upload.
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Step 1: Initialize.
        fileName = "text-" + System.currentTimeMillis() + ".zip";
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
                BUCKET_NAME, fileName);
        InitiateMultipartUploadResult initResponse =
                s3.initiateMultipartUpload(initRequest);

        File file = new File("/Users/gcornwell/Development/test-files/Zip/360mb.zip");
        long contentLength = file.length();
        System.out.println("Upload large file of " + contentLength + " bytes");
        long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                System.out.println("Uploading part " + i + "...");
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(BUCKET_NAME).withKey(fileName)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);
                System.out.println("Uploaded part " + i);

                // Upload part and add response to our list.
                partETags.add(s3.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(BUCKET_NAME,
                    fileName,
                    initResponse.getUploadId(),
                    partETags);

            System.out.println("Completing multipart upload...");
            CompleteMultipartUploadResult result = s3.completeMultipartUpload(compRequest);
            System.out.println("bucket: " + result.getBucketName());
            System.out.println("location: " + result.getLocation());
            System.out.println("etag: " + result.getETag());
            System.out.println("expiration: " + result.getExpirationTime());

        } catch (Exception e) {
            System.err.println("Upload aborted: " + e);
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(
                    BUCKET_NAME, fileName, initResponse.getUploadId()));
        }
        System.out.println("Uploaded large file");
        */
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("s3-upload-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("óáóúñéüäÜö");
        writer.close();

        return file;
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }
}
