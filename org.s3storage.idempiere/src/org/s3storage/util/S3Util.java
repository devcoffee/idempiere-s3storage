/******************************************************************************
 * Product: iDempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 2012 Trek Global                                             *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 *****************************************************************************/

package org.s3storage.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.compiere.model.MStorageProvider;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Util {
	
	public S3Client s3Client;
	
	public static S3Client createS3Client(MStorageProvider prov) {
		String regionStr = prov.get_ValueAsString("S3Region");
		String endpointStr = prov.get_ValueAsString("S3EndPoint");
		boolean isAwsS3 = prov.getURL().contains("amazonaws.com");
		
		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(prov.getUserName(), prov.getPassword());
		StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCreds);
		UrlConnectionHttpClient urlConnection = (UrlConnectionHttpClient) UrlConnectionHttpClient.builder().build();
		
		if (isAwsS3) {		
			return S3Client.builder()
					.region(Region.of(regionStr))
					.credentialsProvider(credentialsProvider)
					.httpClient(urlConnection).build();
		} else {
			return S3Client.builder()
					.region(Region.of(regionStr))
					.endpointOverride(getEndpoint(endpointStr))
					.endpointProvider(null)
					.credentialsProvider(credentialsProvider)
					.forcePathStyle(true)
					.httpClient(urlConnection).build();
		}

	}
	
	/**
	 * Check if the object Exist on Bucket
	 * 
	 * @return Boolean
	 */
	public static boolean exists(S3Client s3Client, String bucket, String key) {
		try {
		       HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
		       HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
		       return headObjectResponse.sdkHttpResponse().isSuccessful();    
		   }
		   catch (NoSuchKeyException e) {
		      //Log exception for debugging
		      return false;
		   }
	}
	
	public static byte[] getObject(S3Client s3Client, String bucket, String key) {
        try {
        	GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
			return s3Client.getObjectAsBytes(getObjectRequest).asByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } 

        return null;
	}
	
	
	public static boolean putObject(S3Client s3Client, String bucket, String path, File file) {
        try {
			PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(bucket)
					.key(path).build();
			s3Client.putObject(objectRequest, RequestBody.fromFile(file));
			return true;
        	 } catch (Exception e) {
            e.printStackTrace();
        }
		return false; 
	}
	
	public static boolean deleteObject(S3Client s3Client, String bucket, String path) {
        try {
			DeleteObjectRequest objectRequest = DeleteObjectRequest.builder().bucket(bucket)
					.key(path).build();
			s3Client.deleteObject(objectRequest);
			return true;
        	 } catch (Exception e) {
            e.printStackTrace();
        }
		return false; 
	}

	/**
	 * Returns the endpoint from URL provided
	 * 
	 * @return URI
	 */
	private static URI getEndpoint(String endpointStr) {
		URI endpoint = null;
		try {
			endpoint = new URI(endpointStr);
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return endpoint;
	}
}