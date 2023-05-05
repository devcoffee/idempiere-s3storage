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

package org.s3storage.idempiere.model;

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

import org.compiere.model.IArchiveStore;
import org.compiere.model.MArchive;
import org.compiere.model.MStorageProvider;
import org.compiere.util.CLogger;
import org.s3storage.idempiere.util.S3Util;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import software.amazon.awssdk.services.s3.S3Client;

public class ArchiveS3Compatible implements IArchiveStore {
		
	private static final CLogger log = CLogger.getCLogger(ArchiveS3Compatible.class);
	
	private  String ARCHIVE_FOLDER_PLACEHOLDER = "%ARCHIVE_FOLDER%";

	//temporary buffer when AD_Archive_ID=0;
	private byte[] buffer;

	@Override
	public byte[] loadLOBData(MArchive archive, MStorageProvider prov) {
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		String archivePathRoot = getArchivePathRoot(prov);
		
		if ("".equals(archivePathRoot)) {
			throw new IllegalArgumentException("no attachmentPath defined");
		}
		buffer = null;
		byte[] data = archive.getByteData();
		if (data == null) {
			return null;
		}

		final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

		try {
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.parse(new ByteArrayInputStream(data));
			final NodeList entries = document.getElementsByTagName("entry");
			if(entries.getLength()!=1){
				log.severe("no archive entry found");
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
					filePath = filePath.replaceFirst(ARCHIVE_FOLDER_PLACEHOLDER, archivePathRoot.replaceAll("\\\\","\\\\\\\\"));
					S3Client s3Client = S3Util.createS3Client(prov);
					if (S3Util.exists(s3Client, bucketStr, filePath)) {
						byte[] dataEntry = S3Util.getObject(s3Client, bucketStr, filePath);
						return dataEntry;
					}
				}
		} catch (SAXException sxe) {
			// Error generated during parsing)
			Exception x = sxe;
			if (sxe.getException() != null)
				x = sxe.getException();
			x.printStackTrace();
			log.severe(x.getMessage());

		} catch (ParserConfigurationException pce) {
			// Parser with specified options can't be built
			pce.printStackTrace();
			log.severe(pce.getMessage());

		} catch (IOException ioe) {
			// I/O error
			ioe.printStackTrace();
			log.severe(ioe.getMessage());
		}
		
		return null;
	}

	@Override
	public void save(MArchive archive, MStorageProvider prov,byte[] inflatedData) {
		
		if (inflatedData == null || inflatedData.length == 0) {
			throw new IllegalArgumentException("InflatedData is NULL");
		}
		if(archive.get_ID()==0){
			//set binary data otherwise save will fail
			archive.setByteData(new byte[]{'0'});
			buffer = inflatedData;
		} else {		
			write(archive, prov, inflatedData);			
		}
	}

	private void write(MArchive archive, MStorageProvider prov, byte[] inflatedData) {		
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		String archivePathRoot = getArchivePathRoot(prov);

		try {
			final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			
			if ("".equals(archivePathRoot)) {
				throw new IllegalArgumentException("no attachmentPath defined");
			}

			StringBuilder msgfile = new StringBuilder().append(archivePathRoot).append(archive.getArchivePathSnippet()).append(archive.get_ID()).append(".pdf");
			S3Client s3Client = S3Util.createS3Client(prov);
			if (!S3Util.putObjectFomBytes(s3Client, bucketStr, msgfile.toString(), inflatedData))
				log.log(Level.SEVERE, "Error on save object | " + msgfile.toString());

			//create xml entry
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.newDocument();
			final Element root = document.createElement("archive");
			document.appendChild(root);
			document.setXmlStandalone(true);
			final Element entry = document.createElement("entry");
			StringBuilder msgsat = new StringBuilder(ARCHIVE_FOLDER_PLACEHOLDER).append(archive.getArchivePathSnippet()).append(archive.get_ID()).append(".pdf");
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
			archive.setByteData(xmlData);

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			throw new RuntimeException(e);
		}
	}

	private String getArchivePathRoot(MStorageProvider prov) {
		String archivePathRoot = prov.getFolder();
		if (archivePathRoot == null)
			archivePathRoot = "";
		if (archivePathRoot.startsWith("/"))
			archivePathRoot = archivePathRoot.replaceFirst("/", "");
		if (!archivePathRoot.endsWith("/"))
			archivePathRoot = archivePathRoot + "/";
		return archivePathRoot;
	}

	@Override
	public boolean deleteArchive(MArchive archive, MStorageProvider prov) {
		String archivePathRoot = getArchivePathRoot(prov);
		String bucketStr = prov.get_ValueAsString("S3Bucket");

		if ("".equals(archivePathRoot)) {
			throw new IllegalArgumentException("no attachmentPath defined");
		}
		StringBuilder msgfile = new StringBuilder().append(archivePathRoot)
				.append(archive.getArchivePathSnippet()).append(archive.getAD_Archive_ID()).append(".pdf");
		
		try {
			S3Client s3Client = S3Util.createS3Client(prov);
			if (S3Util.deleteObject(s3Client, bucketStr, msgfile.toString()))
				return true;
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
	public void flush(MArchive archive, MStorageProvider prov) {
		if (buffer != null && buffer.length > 0) {
			write(archive, prov, buffer);
			buffer = null;
		}
	}

}
