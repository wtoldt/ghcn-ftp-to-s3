package com.wtoldt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import javax.annotation.PostConstruct;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@SpringBootApplication
public class GhcnFtpToS3Application {
	private final static Log LOG = LogFactory.getLog(GhcnFtpToS3Application.class);

	public static void main(String[] args) {
		SpringApplication.run(GhcnFtpToS3Application.class, args);
	}

	@PostConstruct
	public void copySourceData() {
		AWSCredentials awsCredentials = null;
		awsCredentials = new ProfileCredentialsProvider("default").getCredentials();
		final AmazonS3Client s3 = new AmazonS3Client(awsCredentials);
		s3.setRegion(Region.getRegion(Regions.US_WEST_2));
		final String bucketName = "ghcn-source-data";
		if (!s3.doesBucketExist(bucketName)) {

		} else {
			LOG.info("emptying bucket " + bucketName + " ...");
			for (final S3ObjectSummary summary : s3.listObjects(bucketName).getObjectSummaries()) {
				s3.deleteObject(bucketName, summary.getKey());
			}
			LOG.info("bucket empty. Deleting bucket.");
			s3.deleteBucket(bucketName);
		}
		LOG.info("creating bucket " + bucketName);
		s3.createBucket(bucketName);

		final FTPClient ftp = new FTPClient();
		boolean success = false;
		final Instant start = Instant.now();

		LOG.info("connecting and downloading ghcnd_all.tar.gz...");

		try {
			ftp.connect("ftp.ncdc.noaa.gov");
			ftp.login("anonymous", "anonymous@gmail.com");
			ftp.changeWorkingDirectory("pub/data/ghcn/daily/");
			ftp.setConnectTimeout(3000);
			ftp.setDataTimeout(3000);
			ftp.setDefaultTimeout(3000);
			ftp.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
			ftp.setFileType(FTP.BINARY_FILE_TYPE);

			final File file = File.createTempFile("ghcn", "gz");
			file.deleteOnExit();
			final FileOutputStream fos = new FileOutputStream(file);

			success = ftp.retrieveFile("ghcnd_all.tar.gz", fos);
			fos.close();
			LOG.info("ftp success: " + success);
			LOG.info("downloaded file size: " + file.length());

			final TarArchiveInputStream tarInput =
					new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));

			TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
			int i = 1;
			LOG.info("Copying to s3");
			while(currentEntry != null) {

				if (!currentEntry.isDirectory()) {
					final byte[] content = new byte[(int) currentEntry.getSize()];
					tarInput.read(content, 0, content.length);
					final File tempTarFile = File.createTempFile("tmp-ghcn", "dly");
					FileUtils.writeByteArrayToFile(tempTarFile, content);
					s3.putObject(bucketName, currentEntry.getName(), tempTarFile);
				}

				if (i%1000 == 0) {
					LOG.info(i + "files transfered...");
				}
				currentEntry = tarInput.getNextTarEntry();
				i++;
			}
			LOG.info("successfully transfered " + i + " files.");
			tarInput.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
		LOG.info(Duration.between(start, Instant.now()).toString());
	}
}
