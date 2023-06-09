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

package org.devcoffee.idempiere.s3storage.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Level;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.IImageStore;
import org.compiere.model.MImage;
import org.compiere.model.MStorageProvider;
import org.compiere.util.CLogger;
import org.devcoffee.idempiere.s3storage.util.S3Util;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import software.amazon.awssdk.services.s3.S3Client;

public class ImageS3Compatible implements IImageStore {
	
	private static final CLogger log = CLogger.getCLogger(ImageS3Compatible.class);
	
	private  String IMAGE_FOLDER_PLACEHOLDER = "%IMAGE_FOLDER%";
	
	//temporary buffer when AD_Image_ID=0
	private byte[] buffer = null;

	@Override
	public byte[] load(MImage image, MStorageProvider prov) {
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		String imagePathRoot = getImagePathRoot(prov);
		
		if ("".equals(imagePathRoot)) {
			throw new IllegalArgumentException("no path defined");
		}
		buffer = null;
		byte[] data = image.getByteData();
		if (data == null) {
			return null;
		}

		final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

		try {
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.parse(new ByteArrayInputStream(data));
			final NodeList entries = document.getElementsByTagName("entry");
			if(entries.getLength()!=1){
				log.severe("no image entry found");
			}
			final Node entryNode = entries.item(0);
			final NamedNodeMap attributes = entryNode.getAttributes();
			final Node	 fileNode = attributes.getNamedItem("file");
			if(fileNode==null ){
				log.severe("no filename for entry");
				return null;
			}
			String filePath = fileNode.getNodeValue();
			if (log.isLoggable(Level.FINE)) log.fine("filePath: " + filePath);
			if(filePath!=null){
				filePath = filePath.replaceFirst(IMAGE_FOLDER_PLACEHOLDER, imagePathRoot.replaceAll("\\\\","\\\\\\\\"));
				S3Client s3Client = S3Util.createS3Client(prov);
				if (S3Util.exists(s3Client, bucketStr, filePath)) {
					byte[] dataEntry = S3Util.getObject(s3Client, bucketStr, filePath);
					// Return the object if Found
					return dataEntry;
				}
			}
		} catch (SAXException sxe) {
			// Error generated during parsing)
			Exception x = sxe;
			if (sxe.getException() != null)
				x = sxe.getException();
			log.severe(x.getMessage());

		} catch (ParserConfigurationException pce) {
			// Parser with specified options can't be built
			log.severe(pce.getMessage());

		} catch (IOException ioe) {
			// I/O error
			log.severe(ioe.getMessage());
		} catch (Exception e) {
			// General error
			log.severe(e.getMessage());
		}
		
		return null;
	}

	@Override
	public void  save(MImage image, MStorageProvider prov,byte[] inflatedData) {
		if (inflatedData == null || inflatedData.length == 0) {
			image.setByteData(null);
			delete(image, prov);
			return;
		}
		
		if(image.get_ID()==0){
			//set binary data otherwise save will fail
			image.setByteData(new byte[]{'0'});
			buffer = inflatedData;
		} else {
			write(image, prov, inflatedData);
		}
	}

	private void write(MImage image, MStorageProvider prov, byte[] inflatedData) {
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		String imagePathRoot = getImagePathRoot(prov);

		try {
			final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();			
			
			if ("".equals(imagePathRoot)) {
				throw new IllegalArgumentException("no storage path defined");
			}

			StringBuilder msgfile = new StringBuilder().append(imagePathRoot)
					.append(image.getImageStoragePath()).append(image.get_ID());
			S3Client s3Client = S3Util.createS3Client(prov);
			if (!S3Util.putObjectFomBytes(s3Client, bucketStr, msgfile.toString(), inflatedData)) {
				log.log(Level.SEVERE, "Error on save object | " + msgfile.toString());
				throw new AdempiereException("Error saving S3 object: " + image.getName());
			}

			//create xml entry
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.newDocument();
			final Element root = document.createElement("image");
			document.appendChild(root);
			document.setXmlStandalone(true);
			final Element entry = document.createElement("entry");
			StringBuilder msgsat = new StringBuilder(IMAGE_FOLDER_PLACEHOLDER).append(image.getImageStoragePath()).append(image.get_ID());
			entry.setAttribute("file", msgsat.toString());
			root.appendChild(entry);
			final Source source = new DOMSource(document);
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			final Result result = new StreamResult(bos);
			final Transformer xformer = TransformerFactory.newInstance().newTransformer();
			xformer.transform(source, result);
			final byte[] xmlData = bos.toByteArray();
			if (log.isLoggable(Level.FINE)) log.fine(bos.toString());
			//store xml in db
			image.setByteData(xmlData);

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			image.setByteData(null);
			throw new RuntimeException(e);
		}
	}

	private String getImagePathRoot(MStorageProvider prov) {
		String imagePathRoot = prov.getFolder();
		if (imagePathRoot == null)
			imagePathRoot = "";
		if (imagePathRoot.startsWith("/"))
			imagePathRoot = imagePathRoot.replaceFirst("/", "");
		if (!imagePathRoot.endsWith("/"))
			imagePathRoot = imagePathRoot + "/";
		return imagePathRoot;
	}

	@Override
	public boolean delete(MImage image, MStorageProvider prov) {
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		String imagePathRoot = getImagePathRoot(prov);

		if ("".equals(imagePathRoot)) {
			throw new IllegalArgumentException("no attachmentPath defined");
		}
		StringBuilder msgfile = new StringBuilder().append(imagePathRoot)
				.append(image.getImageStoragePath()).append(image.getAD_Image_ID());
		
		try {
			S3Client s3Client = S3Util.createS3Client(prov);
			if (S3Util.deleteObject(s3Client, bucketStr, msgfile.toString())) {
				return true;
			} else {
				throw new AdempiereException("Error deleting S3 object: " + image.getName());
			}
				
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		}
		return false;
	}

	@Override
	public boolean isPendingFlush() {
		return buffer != null && buffer.length > 0;
	}

	@Override
	public void flush(MImage image, MStorageProvider prov) {
		if (buffer != null && buffer.length > 0) {
			write(image, prov, buffer);
			buffer = null;
		}		
	}

}
