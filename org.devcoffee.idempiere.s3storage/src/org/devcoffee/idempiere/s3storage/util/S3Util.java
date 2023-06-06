/******************************************************************************
 * Product: iDempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 2012 devCoffee Soluções em Tecnologia                        *
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

package org.devcoffee.idempiere.s3storage.util;

import java.io.File;
import java.net.URI;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.compiere.model.MStorageProvider;
import org.compiere.util.CLogger;

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

	private static final CLogger log = CLogger.getCLogger(S3Util.class);
	
	private static final Pattern ENDPOINT_PATTERN = Pattern.compile("^(.+\\.)?s3[.-]([a-z0-9-]+)\\.");

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
			return S3Client.builder().region(Region.of(regionStr))
					.endpointOverride(getEndpoint(endpointStr))
					.endpointProvider(null)
					.credentialsProvider(credentialsProvider)
					.forcePathStyle(true)
					.httpClient(urlConnection).build();
		}

	}

	/**
	 * Check if the object exist on Bucket
	 * 
	 * @return Boolean
	 */
	public static boolean exists(S3Client s3Client, String bucket, String key) {
		try {
			HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
			HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
			return headObjectResponse.sdkHttpResponse().isSuccessful();
		} catch (NoSuchKeyException e) {
			log.log(Level.SEVERE, "Object not found", e);
		}
		return false;
	}

	public static byte[] getObject(S3Client s3Client, String bucket, String key) {
		try {
			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
			return s3Client.getObjectAsBytes(getObjectRequest).asByteArray();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		}
		return null;
	}

	public static boolean putObject(S3Client s3Client, String bucket, String path, File file) {
		try {
			PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(bucket).key(path).build();
			s3Client.putObject(objectRequest, RequestBody.fromFile(file));
			return true;
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		}
		return false;
	}
	
	public static boolean putObjectFomBytes(S3Client s3Client, String bucket, String path, byte[] bytes) {
		try {
			PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(bucket).key(path).build();
			s3Client.putObject(objectRequest, RequestBody.fromBytes(bytes));
			return true;
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		}
		return false;
	}

	public static boolean deleteObject(S3Client s3Client, String bucket, String path) {
		try {
			DeleteObjectRequest objectRequest = DeleteObjectRequest.builder().bucket(bucket).key(path).build();
			s3Client.deleteObject(objectRequest);
			return true;
		} catch (Exception e) {
			log.log(Level.SEVERE, "error", e);
		}
		return false;
	}
	
	
	
	/**
	 * Returns the endpoint from URL provided
	 * 
	 * @return URI
	 */
	private static URI getEndpoint(String endpointStr) {
	    URI uri = URI.create(endpointStr);
		return uri;
	}
}