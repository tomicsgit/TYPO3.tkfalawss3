<?php
if (!defined('TYPO3_MODE')) {
	die('Access denied.');
}

// register additional driver
$TYPO3_CONF_VARS['SYS']['fal']['registeredDrivers']['AmazonS3'] = array(
	'class' => 'ThomasKieslich\Tkfalawss3\Driver\AmazonS3Driver',
	'label' => 'Amazon S3',
	'flexFormDS' => 'FILE:EXT:tkfalawss3/Configuration/FlexForm/FlexForm.xml'
);