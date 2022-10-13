package com.bazaarvoice.nn.nataraja.athenadataharvester.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Path;

@Service
public class AmazonS3Service {

    @Value ("${custom.res-bucket-name}")
    private String s3BucketName;
    private AmazonS3 amazonS3Client;

    private ObjectMapper mapper = new ObjectMapper();

//    public void  writeFile(String bucketName , String fileName , InputStream is) {
//        InputStream targetStream = new ByteArrayInputStream(mapper.writeValueAsString(result).getBytes());
//        ObjectMetadata objectMetadata = new ObjectMetadata();
//        PutObjectRequest req = new PutObjectRequest(bucketName, fileName, is,objectMetadata);
//        amazonS3Client.putObject(req);
//    }

//    public void  appendToFile(String bucketName , String fileName , InputStream is){
//        if(getfile){
//
//        }
//
//        ObjectMetadata objectMetadata = new ObjectMetadata();
//        PutObjectRequest req = new PutObjectRequest(bucketName, fileName, is,objectMetadata);
//        amazonS3Client.putObject(req);
//    }


    public boolean  uploadFile(String taskId, String client, String taskFilename, Path filePath)
            throws JsonProcessingException {
        PutObjectRequest request = new PutObjectRequest(s3BucketName, generateFileName(taskId, client, taskFilename), filePath.toFile());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.addUserMetadata("title", "someTitle");
        request.setMetadata(metadata);
        amazonS3Client.putObject(request);

        return true;
    }

    private String generateFileName(String taskId, String client, String taskFilename) {
        String fileName = client + "/" + taskId + "/" + taskFilename;
        return fileName;
    }
}
