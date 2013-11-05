<?php
namespace ThomasKieslich\Tkfalawss3\Driver;

	/***************************************************************
	 *  Copyright notice
	 *
	 *  (c) 2013 Thomas Kieslich
	 *  All rights reserved
	 *
	 *  This script is part of the TYPO3 project. The TYPO3 project is
	 *  free software; you can redistribute it and/or modify
	 *  it under the terms of the GNU General Public License as published by
	 *  the Free Software Foundation; either version 2 of the License, or
	 *  (at your option) any later version.
	 *
	 *  The GNU General Public License can be found at
	 *  http://www.gnu.org/copyleft/gpl.html.
	 *  A copy is found in the textfile GPL.txt and important notices to the license
	 *  from the author is found in LICENSE.txt distributed with these scripts.
	 *
	 *
	 *  This script is distributed in the hope that it will be useful,
	 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
	 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	 *  GNU General Public License for more details.
	 *
	 *  This copyright notice MUST APPEAR in all copies of the script!
	 ***************************************************************/

/**
 * Driver for Amazon Simple Storage Service (S3).
 *
 * @todo Handle exceptions, write tests
 * @todo Enable thumbnail generation
 */
class AmazonS3Driver extends \TYPO3\CMS\Core\Resource\Driver\AbstractDriver {

	const FILTER_ALL = 'all';

	const FILTER_FOLDERS = 'folders';

	const FILTER_FILES = 'files';

	const ROOT_FOLDER_IDENTIFIER = '/';

	const DEBUG_MODE = FALSE;

	/**
	 * @var \Aws\S3\S3Client
	 */
	protected $s3Client;

	/**
	 * The base URL that points to this driver's storage. As long is this is not set, it is assumed that this folder
	 * is not publicly available
	 *
	 * @var \string
	 */
	protected $baseUrl;

	/**
	 * The identifier map used for renaming
	 *
	 * @var \array<\string>
	 */
	protected $identifierMap;

	/**
	 * Object existence is cached here like:
	 * $identifier => TRUE|FALSE
	 *
	 * @var \array<\boolean>
	 */
	protected $objectExistenceCache = array();

	/**
	 * Object permissions are cached here in subarrays like:
	 * $identifier => array('r' => \boolean, 'w' => \boolean)
	 *
	 * @var \array<\array>
	 */
	protected $objectPermissionsCache = array();

	/**
	 * @param array $configuration
	 * @return boolean
	 * @todo implement this
	 */
	public static function verifyConfiguration(array $configuration) {
		return TRUE;
	}

	/**
	 * @return void
	 */
	public function processConfiguration() {
		;
	}

	/**
	 * @return void
	 */
	public function initialize() {
		require_once PATH_site . 'typo3conf/ext/tkfalawss3/Classes/AwsSdk/aws-autoloader.php';

		$this->initializeBaseUrl();
		$this->capabilities = \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_BROWSABLE | \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_PUBLIC | \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_WRITABLE;
		$this->initializeClient();
	}

	/**
	 * @return void
	 */
	public function initializeBaseUrl() {
		$this->baseUrl = \TYPO3\CMS\Core\Utility\GeneralUtility::getIndpEnv('TYPO3_SSL') ? 'https' : 'http' . '://';

		if (isset($this->configuration['publicBaseUrl']) && $this->configuration['publicBaseUrl'] !== '') {
			$this->baseUrl .= $this->configuration['publicBaseUrl'];
		} else {
			$this->baseUrl .= $this->configuration['bucket'] . '.s3.amazonaws.com/';
		}

	}

	/**
	 * @return void
	 * @see http://docs.aws.amazon.com/aws-sdk-php-2/latest/class-Aws.S3.S3Client.html#_factory
	 */
	public function initializeClient() {
		$reflection = new \ReflectionClass('\Aws\Common\Enum\Region');
		$this->s3Client = \Aws\S3\S3Client::factory(array(
			'key' => $this->configuration['key'],
			'secret' => $this->configuration['secretKey'],
			'region' => $reflection->getConstant($this->configuration['region'])
		));
		\Aws\S3\StreamWrapper::register($this->s3Client);
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\ResourceInterface $resource
	 * @param \boolean $relativeToCurrentScript Just to be API conformant, n/a for S3 storage
	 * @return \string
	 */
	public function getPublicUrl(\TYPO3\CMS\Core\Resource\ResourceInterface $resource, $relativeToCurrentScript = FALSE) {
		return $this->baseUrl . $resource->getIdentifier();
	}

	/**
	 * Creates a (cryptographic) hash for a file.
	 *
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param string $hashAlgorithm
	 * @return string
	 */
	public function hash(\TYPO3\CMS\Core\Resource\FileInterface $file, $hashAlgorithm) {
		return $hashAlgorithm($file->getIdentifier());
	}

	/**
	 * @return \TYPO3\CMS\Core\Resource\Folder
	 */
	public function getRootLevelFolder() {
		if (!$this->rootLevelFolder) {
			$this->rootLevelFolder = \TYPO3\CMS\Core\Resource\ResourceFactory::getInstance()->createFolderObject($this->storage, self::ROOT_FOLDER_IDENTIFIER, '');
		}
		return $this->rootLevelFolder;
	}

	/**
	 * Returns the default folder new files should be put into.
	 *
	 * @return \TYPO3\CMS\Core\Resource\Folder
	 */
	public function getDefaultFolder() {
		return $this->getRootLevelFolder();
	}

	/**
	 * Returns information about a file.
	 *
	 * @param string $identifier The (relative) path to the file.
	 * @return array
	 */
	public function getFileInfoByIdentifier($identifier, array $propertiesToExtract = array()) {
		$this->normalizeIdentifier($identifier);
		$metadata = $this->s3Client->headObject(array(
			'Bucket' => $this->configuration['bucket'],
			'Key' => $identifier
		))->toArray();

		return array(
			'name' => basename($identifier),
			'identifier' => $identifier,
			'modificationData' => strtotime($metadata['LastModified']),
			'size' => (integer)$metadata['ContentLength'],
			'storage' => $this->storage->getUid()
		);
	}

	/**
	 * Returns a file by its identifier.
	 *
	 * @param $identifier
	 * @return \TYPO3\CMS\Core\Resource\FileInterface
	 */
	public function getFile($identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($identifier, 'Hello from ' . __METHOD__);
		$this->normalizeIdentifier($identifier);
		return parent::getFile($identifier);
	}

	/**
	 * Returns a list of files inside the specified path
	 *
	 * @param \string $path
	 * @param \string $start
	 * @param \integer $numberOfItems
	 * @param \array $filters
	 * @param \array $rows
	 * @param \boolean $recursive
	 * @return array
	 * @todo handle errors
	 * @todo implement caching
	 * @todo respect $start, $numberOfItems, $filters and $rows parameters
	 */
	public function getFileList($path, $start = 0, $numberOfItems = 0, $filters = array(), $rows = array(), $recursive = FALSE) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($path, 'Hello from ' . __METHOD__);
		$this->normalizeIdentifier($path);
		$files = array();
		if ($path === self::ROOT_FOLDER_IDENTIFIER) {
			$path = '';
		}

		$response = $this->s3Client->listObjects(array(
			'Bucket' => $this->configuration['bucket'],
			'Prefix' => $path
		))->toArray();

		foreach ($response['Contents'] as $fileCandidate) {
			// skip directory entries
			if (substr($fileCandidate['Key'], -1) === '/') {
				continue;
			}

			// skip subdirectory entries
			if (!$recursive && substr_count($fileCandidate['Key'], '/') > substr_count($path, '/')) {
				continue;
			}

			$fileName = basename($fileCandidate['Key']);
			$files[$fileCandidate['Key']] = array(
				'name' => $fileName,
				'identifier' => $fileCandidate['Key'],
				'modificationDate' => strtotime($fileCandidate['lastModified']),
				'size' => (integer)$fileCandidate['Size'],
				'storage' => $this->storage->getUid()
			);
		}

		return $files;
	}

	/**
	 * Returns a list of all folders in a given path
	 *
	 * @param string $path
	 * @param string $pattern
	 * @return array
	 *
	 * @todo handle errors
	 * @todo implement caching
	 */
	public function getFolderList($path, $pattern = '') {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($path, 'Hello from ' . __METHOD__);
		$this->normalizeIdentifier($path);
		$folders = array();

		$configuration = array(
			'Bucket' => $this->configuration['bucket']
		);
		if ($path === self::ROOT_FOLDER_IDENTIFIER) {
			$configuration['Delimiter'] = $path;
			$response = $this->s3Client->listObjects($configuration)->toArray();
			foreach ($response['CommonPrefixes'] as $folderCandidate) {
				$key = $folderCandidate['Prefix'];
				$folderName = basename(rtrim($key, '/'));
				$storageRecord = $this->storage->getStorageRecord();
				if ($folderName !== $storageRecord['processingfolder']) {
					$folders[$folderName] = array(
						'name' => $folderName,
						'identifier' => $key,
						'storage' => $this->storage->getUid()
					);
				}
			}
		} else {
			foreach ($this->getSubObjects($path, FALSE, self::FILTER_FOLDERS) as $folderObject) {
				$key = $folderObject['Key'];
				$folderName = basename(rtrim($key, '/'));
				$folders[$folderName] = array(
					'name' => $folderName,
					'identifier' => $key,
					'storage' => $this->storage->getUid()
				);
			}
		}

		return $folders;
	}

	/**
	 * Returns a folder by its identifier.
	 *
	 * @param $identifier
	 * @return \TYPO3\CMS\Core\Resource\Folder
	 */
	public function getFolder($identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($identifier, 'Hello from ' . __METHOD__);
		if ($identifier === self::ROOT_FOLDER_IDENTIFIER) {
			return $this->getRootLevelFolder();
		}
		$this->normalizeIdentifier($identifier);
		return new \TYPO3\CMS\Core\Resource\Folder($this->storage, $identifier, basename(rtrim($identifier, '/')));
	}

	/**
	 * Checks if a file exists
	 *
	 * @param \string $identifier
	 * @return \bool
	 */
	public function fileExists($identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($identifier, 'Hello from ' . __METHOD__);
		if (substr($identifier, -1) === '/') {
			return FALSE;
		}
		return $this->objectExists($identifier);
	}

	/**
	 * Checks if a folder exists
	 *
	 * @param \string $identifier
	 * @return \boolean
	 */
	public function folderExists($identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($identifier, 'Hello from ' . __METHOD__);
		if ($identifier === self::ROOT_FOLDER_IDENTIFIER) {
			return TRUE;
		}
		if (substr($identifier, -1) !== '/') {
			return FALSE;
		}
		return $this->objectExists($identifier);
	}

	/**
	 * Checks if an object exists
	 *
	 * @param \string $identifier
	 * @return \boolean
	 */
	public function objectExists($identifier) {
		$this->normalizeIdentifier($identifier);
		if (!isset($this->objectExistenceCache[$identifier])) {
			try {
				$result = $this->s3Client->doesObjectExist($this->configuration['bucket'], $identifier);
			} catch (Exception $exc) {
				echo $exc->getTraceAsString();
				$result = FALSE;
			}
			$this->objectExistenceCache[$identifier] = $result;
		}
		return $this->objectExistenceCache[$identifier];
	}

	/**
	 * @param \string $fileName
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @return \boolean
	 */
	public function fileExistsInFolder($fileName, \TYPO3\CMS\Core\Resource\Folder $folder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($fileName => $folder), 'Hello from ' . __METHOD__);
		return $this->objectExists($folder->getIdentifier() . $fileName);
	}

	/**
	 * Checks if a folder exists inside a storage folder
	 *
	 * @param \string $folderName
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @return \boolean
	 */
	public function folderExistsInFolder($folderName, \TYPO3\CMS\Core\Resource\Folder $folder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($folderName => $folder), 'Hello from ' . __METHOD__);
		return $this->objectExists($folder->getIdentifier() . $folderName . '/');
	}

	/**
	 * @param \string $localFilePath
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $fileName
	 * @param \TYPO3\CMS\Core\Resource\AbstractFile $updateFileObject
	 * @return \TYPO3\CMS\Core\Resource\File
	 */
	public function addFile($localFilePath, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $fileName, \TYPO3\CMS\Core\Resource\AbstractFile $updateFileObject = NULL) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($localFilePath, $targetFolder, $fileName, $updateFileObject), 'Hello from ' . __METHOD__);
		$targetIdentifier = $targetFolder->getIdentifier() . $fileName;
		if (is_uploaded_file($localFilePath)) {
			$moveResult = file_put_contents(
				$this->getStreamWrapperPath($targetIdentifier),
				file_get_contents($localFilePath)
			);
		} else {
			$localIdentifier = $localFilePath;
			$this->normalizeIdentifier($localIdentifier);

			if ($this->objectExists($localIdentifier)) {
				$moveResult = rename(
					$this->getStreamWrapperPath($localIdentifier),
					$this->getStreamWrapperPath($targetIdentifier)
				);
			} else {
				$moveResult = file_put_contents(
					$this->getStreamWrapperPath($targetIdentifier),
					file_get_contents($localFilePath)
				);
			}
		}

		$fileInfo = $this->getFileInfoByIdentifier($targetIdentifier);
		if ($updateFileObject) {
			$updateFileObject->updateProperties($fileInfo);
			return $updateFileObject;
		} else {
			$fileObject = $this->getFileObject($fileInfo);
			return $fileObject;
		}
	}

	/**
	 * Adds a file at the specified location. This should only be used internally.
	 *
	 * @param \string $localFilePath
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $targetFileName
	 * @return \boolean
	 */
	public function addFileRaw($localFilePath, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $targetFileName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($localFilePath, $targetFolder, $targetFileName), 'Hello from ' . __METHOD__);
		$targetIdentifier = $targetFolder->getIdentifier() . $targetFileName;
		return file_put_contents(
			$this->getStreamWrapperPath($targetIdentifier),
			file_get_contents($localFilePath)
		);
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $fileName
	 * @return \string
	 */
	public function moveFileWithinStorage(\TYPO3\CMS\Core\Resource\FileInterface $file, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $fileName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($file, $targetFolder, $fileName), 'Hello from ' . __METHOD__);
		$targetIdentifier = $targetFolder->getIdentifier() . $fileName;
		$this->renameObject($file->getIdentifier(), $targetIdentifier);
		return $targetIdentifier;
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $fileName
	 * @return \TYPO3\CMS\Core\Resource\FileInterface
	 */
	public function copyFileWithinStorage(\TYPO3\CMS\Core\Resource\FileInterface $file, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $fileName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($file, $targetFolder, $fileName), 'Hello from ' . __METHOD__);
		$targetIdentifier = $targetFolder->getIdentifier() . $fileName;
		$this->copyObject($file->getIdentifier(), $targetIdentifier);
		return $this->getFile($targetIdentifier);
	}

	/**
	 * @param \string $identifier
	 * @param \string $targetIdentifier
	 */
	public function copyObject($identifier, $targetIdentifier) {
		$this->s3Client->copyObject(array(
			'Bucket' => $this->configuration['bucket'],
			'CopySource' => $this->configuration['bucket'] . '/' . $identifier,
			'Key' => $targetIdentifier
		));
	}

	/**
	 * Deletes a file without access and usage checks. This should only be used internally.
	 *
	 * @param \string $identifier
	 * @return \boolean TRUE if removing the file succeeded
	 */
	public function deleteFileRaw($identifier) {
		return $this->deleteObject($identifier);
	}

	/**
	 * Replaces the contents (and file-specific metadata) of a file object with a local file.
	 *
	 * @param \TYPO3\CMS\Core\Resource\AbstractFile $file
	 * @param \string $localFilePath
	 * @return void
	 * @todo implement this
	 */
	public function replaceFile(\TYPO3\CMS\Core\Resource\AbstractFile $file, $localFilePath) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($localFilePath, $file), 'Hello from ' . __METHOD__);
		die();
	}

	/**
	 * Copies a file to a temporary path and returns that path.
	 *
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @return \string The temporary path
	 * @throws \RuntimeException
	 */
	public function copyFileToTemporaryPath(\TYPO3\CMS\Core\Resource\FileInterface $file) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($file, 'Hello from ' . __METHOD__);
		$sourcePath = $this->getStreamWrapperPath($file);
		$temporaryPath = $this->getTemporaryPathForFile($file);
		$result = copy($sourcePath, $temporaryPath);
		if ($result === FALSE) {
			throw new \RuntimeException('Copying file ' . $file->getIdentifier() . ' to temporary path failed.', 1320577649);
		}
		return $temporaryPath;
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @return \boolean
	 */
	public function deleteFile(\TYPO3\CMS\Core\Resource\FileInterface $file) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($file, 'Hello from ' . __METHOD__);
		$this->deleteObject($file->getIdentifier());
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @param \boolean $deleteRecursively
	 * @return \boolean
	 */
	public function deleteFolder(\TYPO3\CMS\Core\Resource\Folder $folder, $deleteRecursively = FALSE) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($deleteRecursively => $folder), 'Hello from ' . __METHOD__);
		if ($deleteRecursively) {
			$items = $this->s3Client->listObjects(array(
				'Bucket' => $this->configuration['bucket'],
				'Prefix' => $folder->getIdentifier()
			))->toArray();

			foreach ($items['Contents'] as $object) {
				// Filter the folder itself
				if ($object['Key'] !== $folder->getIdentifier()) {
					if (self::is_dir($object['Key'])) {
						$subFolder = $this->getFolder($object['Key']);
						if ($subFolder) {
							$this->deleteFolder($subFolder, $deleteRecursively);
						}
					} else {
						unlink($this->getStreamWrapperPath($object['Key']));
					}
				}
			}
		}

		$this->deleteObject($folder->getIdentifier());
	}

	/**
	 * @param \string $identifier
	 * @return void
	 */
	public function deleteObject($identifier) {
		return $this->s3Client->deleteObject(array(
			'Bucket' => $this->configuration['bucket'],
			'Key' => $identifier
		));
	}

	/**
	 * Returns a (local copy of) a file for processing it.
	 *
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param \boolean $writable
	 * @return \string
	 * @todo take care of replacing the file on change
	 */
	public function getFileForLocalProcessing(\TYPO3\CMS\Core\Resource\FileInterface $file, $writable = TRUE) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($file, 'Hello from ' . __METHOD__);
		return $this->copyFileToTemporaryPath($file);
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @return \array<\boolean>
	 */
	public function getFilePermissions(\TYPO3\CMS\Core\Resource\FileInterface $file) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($file, 'Hello from ' . __METHOD__);
		return $this->getObjectPermissions($file->getIdentifier());
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @return \array<\boolean>
	 */
	public function getFolderPermissions(\TYPO3\CMS\Core\Resource\Folder $folder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($folder, 'Hello from ' . __METHOD__);
		return $this->getObjectPermissions($folder->getIdentifier());
	}

	/**
	 * @param \string $identifier
	 * @return \array<\boolean>
	 */
	public function getObjectPermissions($identifier) {
		if (!isset($this->objectPermissionsCache[$identifier])) {
			if ($identifier === self::ROOT_FOLDER_IDENTIFIER) {
				$permissions = array(
					'r' => TRUE,
					'w' => TRUE,
				);
			} else {
				$permissions = array(
					'r' => FALSE,
					'w' => FALSE,
				);

				$response = $this->s3Client->getObjectAcl(array(
					'Bucket' => $this->configuration['bucket'],
					'Key' => $identifier
				))->toArray();

				// Until the SDK provides any useful information about folder permissions, we take full access for granted as long as one user with full access exists.
				foreach ($response['Grants'] as $grant) {
					if ($grant['Permission'] === \Aws\S3\Enum\Permission::FULL_CONTROL) {
						$permissions['r'] = TRUE;
						$permissions['w'] = TRUE;
					}
				}
			}
			$this->objectPermissionsCache[$identifier] = $permissions;
		}

		return $this->objectPermissionsCache[$identifier];
	}

	/**
	 * @param \string $fileName
	 * @param \TYPO3\CMS\Core\Resource\Folder $parentFolder
	 * @return \TYPO3\CMS\Core\Resource\File
	 */
	public function createFile($fileName, \TYPO3\CMS\Core\Resource\Folder $parentFolder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($fileName => $parentFolder), 'Hello from ' . __METHOD__);
		$identifier = $parentFolder->getIdentifier() . $fileName;
		$this->createObject($identifier);
		$fileInfo = $this->getFileInfoByIdentifier($identifier);
		return $this->getFileObject($fileInfo);
	}

	/**
	 * Creates a new folder by putting a folder dummy object into the storage.
	 * The stream wrapper mkdir function is not applicable because it creates a new bucket.
	 *
	 * @param \string $newFolderName
	 * @param \TYPO3\CMS\Core\Resource\Folder $parentFolder
	 * @return \TYPO3\CMS\Core\Resource\Folder
	 */
	public function createFolder($newFolderName, \TYPO3\CMS\Core\Resource\Folder $parentFolder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($newFolderName => $parentFolder), 'Hello from ' . __METHOD__);
		$newFolderName = trim($newFolderName, '/');

		$identifier = $parentFolder->getIdentifier() . $newFolderName . '/';
		$this->createObject($identifier);
		return \TYPO3\CMS\Core\Resource\ResourceFactory::getInstance()->createFolderObject($this->storage, $identifier, $newFolderName);
	}

	/**
	 * @param \string $identifier
	 * @return void
	 */
	public function createObject($identifier) {
		$this->s3Client->putObject(array(
			'Bucket' => $this->configuration['bucket'],
			'Key' => $identifier
		));
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @return \string
	 */
	public function getFileContents(\TYPO3\CMS\Core\Resource\FileInterface $file) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($file, 'Hello from ' . __METHOD__);

		$result = $this->s3Client->getObject(array(
			'Bucket' => $this->configuration['bucket'],
			'Key' => $file->getIdentifier()
		));
		return (string)$result['Body'];
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param \string $contents
	 * @return \integer
	 */
	public function setFileContents(\TYPO3\CMS\Core\Resource\FileInterface $file, $contents) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($file, $contents), 'Hello from ' . __METHOD__);
		return file_put_contents($this->getStreamWrapperPath($file->getIdentifier()), $contents);
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\FileInterface $file
	 * @param \string $newName
	 * @return \string
	 */
	public function renameFile(\TYPO3\CMS\Core\Resource\FileInterface $file, $newName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($newName => $file), 'Hello from ' . __METHOD__);
		$newIdentifier = $file->getIdentifier();
		$namePivot = strrpos($newIdentifier, $file->getName());
		$newIdentifier = substr($newIdentifier, 0, $namePivot) . $newName;

		$this->renameObject($file->getIdentifier(), $newIdentifier);
		return $newIdentifier;
	}

	/**
	 * Renames a folder by renaming the virtual folder object as well as all its child objects
	 *
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @param \string $newName
	 * @return \array<\string>
	 */
	public function renameFolder(\TYPO3\CMS\Core\Resource\Folder $folder, $newName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($newName => $folder), 'Hello from ' . __METHOD__);
		$this->resetIdentifierMap();

		$parentFolderName = dirname($folder->getIdentifier());
		if ($parentFolderName === '.') {
			$parentFolderName = '';
		} else {
			$parentFolderName .= '/';
		}
		$newIdentifier = $parentFolderName . $newName . '/';

		foreach ($this->getSubObjects($folder->getIdentifier(), FALSE) as $object) {
			$subObjectIdentifier = $object['Key'];
			if (self::is_dir($subObjectIdentifier)) {
				$this->renameSubFolder($this->getFolder($subObjectIdentifier), $newIdentifier);
			} else {
				$newSubObjectIdentifier = $newIdentifier . basename($subObjectIdentifier);
				$this->renameObject($subObjectIdentifier, $newSubObjectIdentifier);
			}
		}

		$this->renameObject($folder->getIdentifier(), $newIdentifier);
		return $this->identifierMap;
	}

	/**
	 * Renames a given subfolder by renaming all its sub objects and the folder itself.
	 * Used for renaming child objects of a renamed a parent object.
	 *
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @param \string $newDirName The new directory name the folder will reside in
	 * @return void
	 */
	public function renameSubFolder(\TYPO3\CMS\Core\Resource\Folder $folder, $newDirName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($newDirName => $folder), 'Hello from ' . __METHOD__);
		foreach ($this->getSubObjects($folder->getIdentifier(), FALSE) as $subObject) {
			$subObjectIdentifier = $subObject['Key'];
			if (self::is_dir($subObjectIdentifier)) {
				$subFolder = $this->getFolder($subObjectIdentifier);
				$this->renameSubFolder($subFolder, $newDirName . $folder->getName() . '/');
			} else {
				$newSubObjectIdentifier = $newDirName . $folder->getName() . '/' . basename($subObjectIdentifier);
				$this->renameObject($subObjectIdentifier, $newSubObjectIdentifier);
			}
		}

		$newIdentifier = $newDirName . $folder->getName() . '/';
		$this->renameObject($folder->getIdentifier(), $newIdentifier);
	}

	/**
	 * Returns all sub objects for the parent object given by identifier, excluding the parent object itself.
	 * If the $recursive flag is disabled, only objects on the exact next level are returned.
	 *
	 * @param \string $identifier
	 * @param \boolean $recursive
	 * @return \array
	 */
	public function getSubObjects($identifier, $recursive = TRUE, $filter = self::FILTER_ALL) {
		$result = $this->s3Client->listObjects(array(
			'Bucket' => $this->configuration['bucket'],
			'Prefix' => $identifier
		))->toArray();

		return array_filter(
			$result['Contents'],
			function (&$object) use ($identifier, $recursive, $filter) {
				return (
					$object['Key'] !== $identifier
					&& ($recursive
						|| substr_count(
							trim(
								str_replace($identifier, '', $object['Key']),
								'/'
							),
							'/'
						) === 0)
					&& ($filter === \ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver::FILTER_ALL
						|| $filter === \ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver::FILTER_FOLDERS && \ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver::is_dir($object['Key'])
						|| $filter === \ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver::FILTER_FILES && !\ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver::is_dir($object['Key'])
					)
				);
			}
		);
	}

	/**
	 * Renames an object using the StreamWrapper
	 *
	 * @param \string $identifier
	 * @param \string $newIdentifier
	 * @return void
	 */
	public function renameObject($identifier, $newIdentifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($identifier, $newIdentifier), 'Hello from ' . __METHOD__);
		rename($this->getStreamWrapperPath($identifier), $this->getStreamWrapperPath($newIdentifier));
		$this->identifierMap[$identifier] = $newIdentifier;
	}

	/**
	 * Folder equivalent to moveFileWithinStorage().
	 *
	 * @param \TYPO3\CMS\Core\Resource\Folder $folderToMove
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $newFolderName
	 * @return \array<\string> The identifier map
	 */
	public function moveFolderWithinStorage(\TYPO3\CMS\Core\Resource\Folder $folderToMove, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $newFolderName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($folderToMove, $targetFolder, $newFolderName), 'Hello from ' . __METHOD__);
		$this->resetIdentifierMap();

		$newIdentifier = $targetFolder->getIdentifier() . $newFolderName . '/';
		$this->renameObject($folderToMove->getIdentifier(), $newIdentifier);

		$subObjects = $this->getSubObjects($folderToMove->getIdentifier());
		$this->sortObjectsForNestedFolderOperations($subObjects);

		foreach ($subObjects as $subObject) {
			$newIdentifier = $targetFolder->getIdentifier() . $newFolderName . '/' . substr($subObject['Key'], strlen($folderToMove->getIdentifier()));
			$this->renameObject($subObject['Key'], $newIdentifier);
		}
		return $this->identifierMap;
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\Folder $folderToCopy
	 * @param \TYPO3\CMS\Core\Resource\Folder $targetFolder
	 * @param \string $newFolderName
	 * @return \boolean
	 */
	public function copyFolderWithinStorage(\TYPO3\CMS\Core\Resource\Folder $folderToCopy, \TYPO3\CMS\Core\Resource\Folder $targetFolder, $newFolderName) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($folderToCopy, $targetFolder, $newFolderName), 'Hello from ' . __METHOD__);

		$newIdentifier = $targetFolder->getIdentifier() . $newFolderName . '/';
		$this->copyObject($folderToCopy->getIdentifier(), $newIdentifier);

		$subObjects = $this->getSubObjects($folderToCopy->getIdentifier());
		$this->sortObjectsForNestedFolderOperations($subObjects);

		foreach ($subObjects as $subObject) {
			$newIdentifier = $targetFolder->getIdentifier() . $newFolderName . '/' . substr($subObject['Key'], strlen($folderToCopy->getIdentifier()));
			$this->copyObject($subObject['Key'], $newIdentifier);
		}

		return TRUE;
	}

	/**
	 * Returns a folder within the given folder.
	 *
	 * @param \string $name
	 * @param \TYPO3\CMS\Core\Resource\Folder $parentFolder
	 * @return \TYPO3\CMS\Core\Resource\Folder
	 */
	public function getFolderInFolder($name, \TYPO3\CMS\Core\Resource\Folder $parentFolder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($name, $parentFolder), 'Hello from ' . __METHOD__);
		$folderIdentifier = $parentFolder->getIdentifier() . $name . '/';
		return $this->getFolder($folderIdentifier);
	}

	/**
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @return \boolean
	 */
	public function isFolderEmpty(\TYPO3\CMS\Core\Resource\Folder $folder) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($folder, 'Hello from ' . __METHOD__);
		$result = $this->s3Client->listObjects(array(
			'Bucket' => $this->configuration['bucket'],
			'Prefix' => $folder->getIdentifier()
		))->toArray();

		// Contents will always include the folder itself
		if (sizeof($result['Contents']) > 1) {
			return FALSE;
		}
		return TRUE;
	}

	/**
	 * Checks if a given identifier is within a container, e.g. if a file or folder is within another folder.
	 *
	 * @param \TYPO3\CMS\Core\Resource\Folder $folder
	 * @param \string $identifier
	 * @return \boolean
	 */
	public function isWithin(\TYPO3\CMS\Core\Resource\Folder $folder, $identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump(array($identifier => $folder), 'Hello from ' . __METHOD__);
		return $this->objectExists($folder->getIdentifier() . $identifier);;
	}

	/**
	 * Checks if a resource exists - does not care for the type (file or folder), since S3 doesn't distinguish them anyway.
	 *
	 * @param \string $identifier
	 * @return \boolean
	 */
	public function resourceExists($identifier) {
		if (self::DEBUG_MODE) \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump('Hello from ' . __METHOD__, $identifier);
		return $this->objectExists($identifier);
	}

	/**
	 * @return void
	 */
	public function resetIdentifierMap() {
		$this->identifierMap = array();
	}

	/**
	 * @param \string &$identifier
	 */
	public function normalizeIdentifier(&$identifier) {
		if ($identifier !== '/') {
			$identifier = ltrim($identifier, '/');
		}
	}

	/**
	 * @param \array<\array> $objects S3 Objects as arrays with at least the Key field set
	 * @return void
	 */
	public function sortObjectsForNestedFolderOperations(array& $objects) {
		usort($objects, function ($object1, $object2) {
			if (substr($object1['Key'], -1) === '/') {
				if (substr($object2['Key'], -1) === '/') {
					$numSlashes1 = substr_count($object1['Key'], '/');
					$numSlashes2 = substr_count($object2['Key'], '/');
					return $numSlashes1 < $numSlashes2 ? -1 : ($numSlashes1 === $numSlashes2 ? 0 : 1);
				} else {
					return -1;
				}
			} else {
				if (substr($object2['Key'], -1) === '/') {
					return 1;
				} else {
					$numSlashes1 = substr_count($object1['Key'], '/');
					$numSlashes2 = substr_count($object2['Key'], '/');
					return $numSlashes1 < $numSlashes2 ? -1 : ($numSlashes1 === $numSlashes2 ? 0 : 1);
				}
			}
		});
	}

	/**
	 * Returns the StreamWrapper path of a file or folder.
	 *
	 * @param \TYPO3\CMS\Core\Resource\FileInterface|\TYPO3\CMS\Core\Resource\Folder|string $file
	 * @return string
	 */
	public function getStreamWrapperPath($file) {
		$basePath = 's3://' . $this->configuration['bucket'] . '/';
		if ($file instanceof \TYPO3\CMS\Core\Resource\FileInterface) {
			$identifier = $file->getIdentifier();
		} elseif ($file instanceof \TYPO3\CMS\Core\Resource\Folder) {
			$identifier = $file->getIdentifier();
		} elseif (is_string($file)) {
			$identifier = $file;
		} else {
			throw new \RuntimeException('Type "' . gettype($file) . '" is not supported.', 1325191178);
		}
		$this->normalizeIdentifier($identifier);
		return $basePath . $identifier;
	}

	/**
	 * Returns whether the object defined by its identifier is a folder
	 *
	 * @param \string $identifier
	 * @return \boolean
	 */
	public static function is_dir($identifier) {
		return substr($identifier, -1) === '/';
	}
}

?>