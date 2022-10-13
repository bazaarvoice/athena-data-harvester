package com.bazaarvoice.nn.nataraja.athenadataharvester.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AWSConfig {

    @Value("${cloud.aws.region.static}")
    private String s3RegionName;

    @Value ("${iam-arn}")
    private String iamArn;

    public AWSCredentials credentials() {
        AWSCredentials credentials = new BasicAWSCredentials(
                "accesskey",
                "secretKey"
        );
        return credentials;
    }

    @Bean
    public AmazonS3 amazonS3() {
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials()))
                .withRegion(s3RegionName)
                .build();
        return s3client;
    }

    @Bean
    public AmazonS3 getAmazonS3Client() {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(awsCredentialsProvider() )
                .withRegion(s3RegionName)
                .build();
    }

    @Bean
    public AWSCredentialsProvider awsCredentialsProvider() {
        if (System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != null) {
            //return WebIdentityTokenCredentialsProvider.builder().build();
        }
        return new DefaultAWSCredentialsProviderChain();
    }

}