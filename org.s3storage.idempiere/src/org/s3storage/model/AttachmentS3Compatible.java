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

package org.s3storage.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.compiere.model.IAttachmentStore;
import org.compiere.model.MAttachment;
import org.compiere.model.MAttachmentEntry;
import org.compiere.model.MStorageProvider;
import org.compiere.util.CLogger;
import org.compiere.util.Util;
import org.s3storage.util.S3Util;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import software.amazon.awssdk.services.s3.S3Client;

public class AttachmentS3Compatible implements IAttachmentStore {

	private static final CLogger log = CLogger.getCLogger(AttachmentS3Compatible.class);

	@Override
	public boolean loadLOBData(MAttachment attach, MStorageProvider prov) {

		String attachmentPathRoot = getAttachmentPathRoot(prov);
		String bucketStr = prov.get_ValueAsString("S3Bucket");

		if (Util.isEmpty(attachmentPathRoot)) {
			log.severe("no attachmentPath defined");
			return false;
		}

		// Reset
		attach.m_items = new ArrayList<MAttachmentEntry>();
		//
		byte[] data = attach.getBinaryData();
		if (data == null)
			return true;
		if (log.isLoggable(Level.FINE))
			log.fine("TextFileSize=" + data.length);
		if (data.length == 0)
			return true;

		// Get the files record
		NodeList entries = getEntriesFromXML(data);
		if (entries == null)
			return true;
		
		for (int i = 0; i < entries.getLength(); i++) {
			final Node entryNode = entries.item(i);
			final NamedNodeMap attributes = entryNode.getAttributes();
			final Node fileNode = attributes.getNamedItem("file");
			final Node nameNode = attributes.getNamedItem("name");
			if (fileNode == null || nameNode == null) {
				log.severe("no filename for entry " + i);
				attach.m_items = null;
				return false;
			}

			//Fix the placeholder of path
			String filePath = fileNode.getNodeValue();
			filePath = filePath.replaceFirst(attach.ATTACHMENT_FOLDER_PLACEHOLDER, attachmentPathRoot.replaceAll("\\\\","\\\\\\\\"));

			try {
				S3Client s3Client = S3Util.createS3Client(prov);
				if (S3Util.exists(s3Client, bucketStr, filePath)) {
					byte[] dataEntry = S3Util.getObject(s3Client, bucketStr, filePath);
					MAttachmentEntry entry = new MAttachmentEntry(nameNode.getNodeValue(), dataEntry, attach.m_items.size() + 1);
					attach.m_items.add(entry);
				} else {
					MAttachmentEntry entry = new MAttachmentEntry("~" + nameNode.getNodeValue()  + "~", "".getBytes(), attach.m_items.size() + 1);
					attach.m_items.add(entry);
				}
					
			} catch (Exception e) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean save(MAttachment attach, MStorageProvider prov) {
		
		String attachmentPathRoot = getAttachmentPathRoot(prov);
		String bucketStr = prov.get_ValueAsString("S3Bucket");

		if (Util.isEmpty(attachmentPathRoot)) {
			log.severe("no attachmentPath defined");
			return false;
		}

		if (attach.m_items == null || attach.m_items.size() == 0) {
			attach.setBinaryData(null);
			return true;
		}

		NodeList xmlEntries = null;

		final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try {
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.newDocument();
			final Element root = document.createElement("attachments");
			document.appendChild(root);
			document.setXmlStandalone(true);
			// create xml entries
			for (int i = 0; i < attach.m_items.size(); i++) {
				if (log.isLoggable(Level.FINE))
					log.fine(attach.m_items.get(i).toString());
				File entryFile = attach.m_items.get(i).getFile();
				if (entryFile == null) {
					String itemName = attach.m_items.get(i).getName();
					if (itemName.startsWith("~") && itemName.endsWith("~")) {
						itemName = itemName.substring(1, itemName.length() - 1);
						if (xmlEntries != null) {
							for (int x = 0; x < xmlEntries.getLength(); x++) {
								final Node entryNode = xmlEntries.item(x);
								final NamedNodeMap attributes = entryNode.getAttributes();
								final Node fileNode = attributes.getNamedItem("file");
								final Node nameNode = attributes.getNamedItem("name");
								if (itemName.equals(nameNode.getNodeValue())) {
									// file was not found but we preserve the old location just in case is temporary
									final Element entry = document.createElement("entry");
									entry.setAttribute("name", itemName);
									entry.setAttribute("file", fileNode.getNodeValue());
									root.appendChild(entry);
									break;
								}
							}
						}
						continue;
					} else
						throw new AdempiereException("Attachment file not found: " + itemName);
				}
				final String path = entryFile.getAbsolutePath();
				// if local file - copy to central attachment folder
				if (log.isLoggable(Level.FINE))
					log.fine(path + " - " + attachmentPathRoot);
				if (!Util.isEmpty(path)) {
					if (log.isLoggable(Level.FINE))
						log.fine("move file to S3 compatible Storage: " + path);
					try {

						// Define the full path of file
						StringBuilder msgfile = new StringBuilder().append(attachmentPathRoot)
								.append(getAttachmentPathSnippet(attach)).append(entryFile.getName());

						// Upload File to S3 Storage
						S3Client s3Client = S3Util.createS3Client(prov);
						if (S3Util.putObject(s3Client, bucketStr, msgfile.toString(), entryFile)) {
							final Element entry = document.createElement("entry");
							entry.setAttribute("name", attach.getEntryName(i));
							String filePathToStore = msgfile.toString();
							filePathToStore = filePathToStore.replaceFirst(
									attachmentPathRoot.replaceAll("\\\\", "\\\\\\\\"),
									attach.ATTACHMENT_FOLDER_PLACEHOLDER);
							log.fine(filePathToStore);
							entry.setAttribute("file", filePathToStore);
							root.appendChild(entry);
						}
						
					} catch (Exception e) {
						e.printStackTrace();
						log.severe("unable to copy file " + entryFile.getAbsolutePath() + " to " + attachmentPathRoot
								+ File.separator + getAttachmentPathSnippet(attach) + File.separator
								+ entryFile.getName());
					} finally {
					}
				}
			}

			final Source source = new DOMSource(document);
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			final Result result = new StreamResult(bos);
			final Transformer xformer = TransformerFactory.newInstance().newTransformer();
			xformer.transform(source, result);
			final byte[] xmlData = bos.toByteArray();
			if (log.isLoggable(Level.FINE))
				log.fine(bos.toString());
			attach.setBinaryData(xmlData);
			attach.setTitle(MAttachment.XML);
			return true;
		} catch (Exception e) {
			log.log(Level.SEVERE, "saveLOBData", e);
		}
		attach.setBinaryData(null);
		return false;

	}

	@Override
	public boolean delete(MAttachment attach, MStorageProvider provider) {
		while (attach.m_items.size() > 0) {
			deleteEntry(attach, provider, attach.m_items.size()-1);
		}
		return true;
	}

	@Override
	public boolean deleteEntry(MAttachment attach, MStorageProvider prov, int index) {
		
		String attachmentPathRoot = getAttachmentPathRoot(prov);
		String bucketStr = prov.get_ValueAsString("S3Bucket");
		
		final MAttachmentEntry entry = attach.m_items.get(index);
		
		StringBuilder path = new StringBuilder(attachmentPathRoot)
				.append(getAttachmentPathSnippet(attach))
				.append(entry.getName());
		
		S3Client s3Client = S3Util.createS3Client(prov);
		S3Util.deleteObject(s3Client, bucketStr, path.toString());
		attach.m_items.remove(index);
		if (attach.get_ID() > 0) // the attachment has not been deleted
			attach.saveEx(); // must save here as the operation cannot be rolled back on filesystem
		if (log.isLoggable(Level.CONFIG)) log.config("Index=" + index + " - NewSize=" + attach.m_items.size());
		return true;
	}

	/**
	 * Get the entries from the XML
	 * 
	 * @param data
	 * @return
	 */
	private NodeList getEntriesFromXML(byte[] data) {
		NodeList entries = null;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			final DocumentBuilder builder = factory.newDocumentBuilder();
			final Document document = builder.parse(new ByteArrayInputStream(data));
			entries = document.getElementsByTagName("entry");
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
		return entries;
	}

	/**
	 * Returns a path snippet, containing client, org, table and record id.
	 * 
	 * @return String
	 */
	private String getAttachmentPathSnippet(MAttachment attach) {
		StringBuilder pathSnippet = new StringBuilder().append(attach.getAD_Client_ID()).append("/")
				.append(attach.getAD_Org_ID()).append("/").append(attach.getAD_Table_ID()).append("/")
				.append(attach.getRecord_ID()).append("/");
		return pathSnippet.toString();
	}

	/**
	 * Returns the path root
	 * 
	 * @return String
	 */
	private String getAttachmentPathRoot(MStorageProvider prov) {
		String attachmentPathRoot = prov.getFolder();
		if (attachmentPathRoot == null)
			attachmentPathRoot = "";
		if (attachmentPathRoot.startsWith("/"))
			attachmentPathRoot = attachmentPathRoot.replaceFirst("/", "");
		if (!attachmentPathRoot.endsWith("/"))
			attachmentPathRoot = attachmentPathRoot + "/";
		return attachmentPathRoot;
	}
	
}
