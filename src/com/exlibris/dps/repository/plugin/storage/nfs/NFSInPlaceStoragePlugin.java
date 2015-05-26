package com.exlibris.dps.repository.plugin.storage.nfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.Checksummer;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
import com.exlibris.core.sdk.storage.handler.StorageUtil;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.common.storage.Fixity.FixityAlgorithm;

public class NFSInPlaceStoragePlugin extends AbstractStorageHandler {

	private static final ExLogger log = ExLogger.getExLogger(NFSInPlaceStoragePlugin.class);
	private static final String NOT_SOFT_LINK_ERROR_MSG ="Failed to store file {0} : \"{1}\" is not a soft link.";
	private static final String FAILED_FIXITY_ERROR_MSG ="Failed to store file {0} : Fixity check on \"{1}\" failed.";

	@Override
	public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata) throws Exception {

		String errorMessage = null;
		String tmpDestinationsFilePath = getTempStorageDirectory(false) + StorageUtil.DEST_PATH_FOLDER;
		String filePid = storedEntityMetadata.getEntityPid();
		String iePid = storedEntityMetadata.getIePid();

		String fileDestination = StorageUtil.readDestPathFromTmpFile(iePid, tmpDestinationsFilePath, filePid);
		if (fileDestination == null) {
			File fileInRepository = new File(storedEntityMetadata.getCurrentFilePath());
			if (Files.isSymbolicLink(fileInRepository.toPath())) {
				File fileInDeposit = Files.readSymbolicLink(fileInRepository.toPath()).toFile();
				if (Files.isSymbolicLink(fileInDeposit.toPath())) {
					Path originalFilePath = Files.readSymbolicLink(fileInDeposit.toPath());
					File originalFile = originalFilePath.toFile();
					String newFileName = createFileName(storedEntityMetadata);
					File newFileDestination = new File(originalFile.getParent() + "/" + newFileName);
					originalFile.renameTo(newFileDestination);
					StorageUtil.saveDestPathToTmpFile(iePid, tmpDestinationsFilePath, filePid, newFileDestination.getAbsolutePath());
					fileDestination = newFileDestination.getAbsolutePath();
				} else {
					errorMessage = MessageFormat.format(NOT_SOFT_LINK_ERROR_MSG, filePid, fileInDeposit.getAbsolutePath());
				}
			} else {
				errorMessage = MessageFormat.format(NOT_SOFT_LINK_ERROR_MSG, filePid, fileInRepository.getAbsolutePath());
			}
		}

		if (fileDestination != null && !checkFixity(storedEntityMetadata.getFixities(), fileDestination)) {
			errorMessage =  MessageFormat.format(FAILED_FIXITY_ERROR_MSG, filePid, fileDestination);
		}

		if (errorMessage != null) {
			log.error(errorMessage);
			throw new Exception(errorMessage);
		}

		return fileDestination;
	}

	@Override
	public InputStream retrieveEntity(String storedEntityIdentifier) throws IOException {
		return new FileInputStream(storedEntityIdentifier);
	}

	@Override
	public boolean deleteEntity(String storedEntityIdentifier) throws IOException {
		File file = new File(storedEntityIdentifier);
		try {
			return file.delete();
		} catch(Exception e) {
			log.warn("Failed to delete entity file : " + file.getPath());
			return false;
		}
	}

	@Override
	public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier) throws Exception {
		boolean fixityResult = true;
		List<Fixity> knownFixities = new ArrayList<>();
		List<FixityAlgorithm> knownFixitiesAlgorithms = new ArrayList<>();
		if (fixities != null) {
			for (Fixity fixity : fixities) {
				String fixityAlgorithm = fixity.getAlgorithm();
				fixity.setResult(null);

				FixityAlgorithm knownAlgorithem = isKnownFixityAlgorithm(fixityAlgorithm);  // Fixity algorithm is MD5 / SHA1 / CRC32
				if (knownAlgorithem != null) {
					knownFixitiesAlgorithms.add(knownAlgorithem);
					knownFixities.add(fixity);
				} else {
					String oldValue = fixity.getValue();
					String newValue = getChecksumUsingPlugin(storedEntityIdentifier, fixity.getPluginName(), oldValue);
					fixity.setResult((oldValue == null) || (oldValue.equals(newValue)));
					fixity.setValue(newValue);
					fixityResult &= fixity.getResult();
				}
			}
			if (!knownFixities.isEmpty()) {
				InputStream inputStream = null;
				try {
					inputStream = retrieveEntity(storedEntityIdentifier);
					Checksummer checksummer = new Checksummer(inputStream, knownFixitiesAlgorithms.contains(FixityAlgorithm.MD5), knownFixitiesAlgorithms.contains(FixityAlgorithm.SHA1), knownFixitiesAlgorithms.contains(FixityAlgorithm.CRC32));
					for (Fixity fixity : knownFixities) {
						String oldValue = fixity.getValue();
						String newValue = checksummer.getChecksum(fixity.getAlgorithm());
						fixity.setResult((oldValue == null) || (oldValue.equalsIgnoreCase(newValue)));
						fixity.setValue(newValue);
						fixityResult &= fixity.getResult();
					}
				} finally {
					if (inputStream != null) {
						inputStream.close();
					}
				}
			}
		}
		return fixityResult;
	}

	private FixityAlgorithm isKnownFixityAlgorithm (String fixityAlgorithm) {
		try {
			return FixityAlgorithm.valueOf(fixityAlgorithm);
		}
		catch (Exception e) {
			return null;
		}
	}

	@Override
	public String getFullFilePath(String storedEntityIdentifier) {
		return storedEntityIdentifier;
	}

	@Override
	public String getLocalFilePath(String storedEntityIdentifier) {
		return getFullFilePath(storedEntityIdentifier);
	}

	@Override
	public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end) throws Exception {
		byte[] bytes = new byte[(int)(end-start+1)];
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(storedEntityIdentifier, "r");
			file.seek(start);
			file.readFully(bytes, 0, (int)(end-start+1));
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (Exception e) {
					log.warn("Failed closing file.");
				}
			}
		}
		return bytes;
	}

}
